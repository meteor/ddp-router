use bson::{Bson, Document};
use serde_json::{Map, Value};
use std::cmp::Ordering;

pub fn cmp_document(
    sort: Option<&Document>,
    lhs: &Map<String, Value>,
    rhs: &Map<String, Value>,
) -> Ordering {
    sort.into_iter()
        .flatten()
        .fold(Ordering::Equal, |ordering, (key, direction)| {
            ordering.then_with(|| {
                let ordering = match (lhs.get(key), rhs.get(key)) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (Some(lhs), Some(rhs)) => cmp_value(lhs, rhs),
                };

                match direction {
                    Bson::Int32(-1) => ordering.reverse(),
                    _ => ordering,
                }
            })
        })
}

pub fn cmp_value_partial(lhs: &Value, rhs: &Value) -> Result<Ordering, Ordering> {
    let lhs_type = value_type(lhs);
    let rhs_type = value_type(rhs);
    if lhs_type != rhs_type {
        return Err(value_type_order(lhs_type).cmp(&value_type_order(rhs_type)));
    }

    Ok(match (lhs, rhs) {
        (Value::Array(lhs), Value::Array(rhs)) => {
            let ordering = lhs.len().cmp(&rhs.len());
            if ordering.is_ne() {
                return Ok(ordering);
            }

            for (lhs, rhs) in lhs.iter().zip(rhs.iter()) {
                let ordering = cmp_value_partial(lhs, rhs)?;
                if ordering.is_ne() {
                    return Ok(ordering);
                }
            }

            Ordering::Equal
        }
        (Value::Bool(lhs), Value::Bool(rhs)) => lhs.cmp(rhs),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Number(lhs), Value::Number(rhs)) => lhs
            .as_f64()
            .partial_cmp(&rhs.as_f64())
            .unwrap_or(Ordering::Equal),
        (Value::Object(lhs), Value::Object(rhs)) => {
            let ordering = lhs.len().cmp(&rhs.len());
            if ordering.is_ne() {
                return Ok(ordering);
            }

            // TODO: Is it OK for BSON-specific types encoded as EJSON?
            for (lhs, rhs) in lhs.iter().zip(rhs.iter()) {
                let ordering = lhs.0.cmp(rhs.0);
                if ordering.is_ne() {
                    return Ok(ordering);
                }

                let ordering = cmp_value_partial(lhs.1, rhs.1)?;
                if ordering.is_ne() {
                    return Ok(ordering);
                }
            }

            Ordering::Equal
        }
        (Value::String(lhs), Value::String(rhs)) => lhs.cmp(rhs),
        _ => unreachable!(),
    })
}

pub fn cmp_value(lhs: &Value, rhs: &Value) -> Ordering {
    match cmp_value_partial(lhs, rhs) {
        Ok(ordering) | Err(ordering) => ordering,
    }
}

// TODO: It coerces all numbers into `1`, just like Meteor. It would be better
// to support `double`, `int`, `long`, and `decimal` separately.
/// <https://www.mongodb.com/docs/manual/reference/operator/query/type/#available-types>
fn value_type(value: &Value) -> i8 {
    match value {
        Value::Array(_) => 4,
        Value::Bool(_) => 8,
        Value::Null => 10,
        Value::Number(_) => 1,
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().map(String::as_str).collect();
            if keys.len() <= 2 {
                keys.sort_unstable();
                match keys.as_slice() {
                    ["$InfNaN"] => return 1,
                    ["$binary"] => return 5,
                    ["$date"] => return 9,
                    ["$regexp", "$flags"] => return 11,
                    ["$type", "$value"] => match object.get("$type").and_then(Value::as_str) {
                        Some("Decimal") => return 1,
                        Some("oid") => return 7,
                        _ => {}
                    },
                    _ => {}
                }
            }

            3
        }
        Value::String(_) => 2,
    }
}

/// <https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/>
fn value_type_order(value_type: i8) -> u8 {
    match value_type {
        -1 => 0,
        10 => 1,
        1 | 16 | 18 | 19 => 2,
        2 | 14 => 3,
        3 => 4,
        4 => 5,
        5 => 6,
        7 => 7,
        8 => 8,
        9 => 9,
        17 => 10,
        11 => 11,
        127 => 12,
        _ => unreachable!(),
    }
}

pub fn is_supported(sort: Option<&Document>) -> bool {
    let Some(sort) = sort else {
        return true;
    };

    sort.iter().all(|(key, direction)| {
        // TODO: Implement nested keys.
        if key.contains('.') {
            return false;
        }

        matches!(direction, Bson::Int32(-1 | 1))
    })
}

#[cfg(test)]
mod tests {
    use super::{cmp_document, is_supported};
    use bson::doc;
    use serde_json::{json, Value};
    use std::cmp::Ordering;

    macro_rules! test {
        ($name:ident, { $($sort:tt)* }, { $($lhs:tt)* }, { $($rhs:tt)* }, $expected:expr) => {
            #[test]
            fn $name() {
                let sort = doc! { $($sort)* };
                let lhs = json! {{ $($lhs)* }};
                let rhs = json! {{ $($rhs)* }};

                let sort = Some(&sort);
                let Value::Object(lhs) = lhs else { unreachable!() };
                let Value::Object(rhs) = rhs else { unreachable!() };

                assert!(is_supported(sort), "{sort:?} is not supported");
                assert_eq!(cmp_document(sort, &lhs, &rhs), $expected);
            }
        };
    }

    macro_rules! eq {($name:ident, { $($sort:tt)* }, { $($lhs:tt)* }, { $($rhs:tt)* }) => {test!($name, { $($sort)* }, { $($lhs)* }, { $($rhs)* }, Ordering::Equal);}}
    macro_rules! ge {($name:ident, { $($sort:tt)* }, { $($lhs:tt)* }, { $($rhs:tt)* }) => {test!($name, { $($sort)* }, { $($lhs)* }, { $($rhs)* }, Ordering::Greater);}}
    macro_rules! le {($name:ident, { $($sort:tt)* }, { $($lhs:tt)* }, { $($rhs:tt)* }) => {test!($name, { $($sort)* }, { $($lhs)* }, { $($rhs)* }, Ordering::Less);}}

    eq!(basic_1, {"a": 1}, {}, {});
    ge!(basic_2, {"a": 1}, {"a": 1}, {});
    le!(basic_3, {"a": 1}, {}, {"a": 1});
}
