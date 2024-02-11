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
        (lhs, rhs) => return Err(cmp_value_order(lhs).cmp(&cmp_value_order(rhs))),
    })
}

pub fn cmp_value(lhs: &Value, rhs: &Value) -> Ordering {
    match cmp_value_partial(lhs, rhs) {
        Ok(ordering) | Err(ordering) => ordering,
    }
}

/// <https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/>
fn cmp_value_order(value: &Value) -> usize {
    match value {
        Value::Null => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        // TODO: Handle `BinData`, `Date`, `ObjectId`, `RegularExpression`, and `Timestamp`.
        Value::Object(_) => 5,
        Value::Array(_) => 6,
        Value::Bool(_) => 9,
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
