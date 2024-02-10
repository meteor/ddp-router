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

pub fn cmp_value(lhs: &Value, rhs: &Value) -> Ordering {
    match (lhs, rhs) {
        (Value::Array(lhs), Value::Array(rhs)) => {
            let ordering = lhs.len().cmp(&rhs.len());
            if ordering.is_ne() {
                return ordering;
            }

            lhs.iter()
                .zip(rhs.iter())
                .fold(Ordering::Equal, |ordering, (lhs, rhs)| {
                    ordering.then_with(|| cmp_value(lhs, rhs))
                })
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
                return ordering;
            }

            // TODO: Is it OK for BSON-specific types encoded as EJSON?
            lhs.iter()
                .zip(rhs.iter())
                .fold(Ordering::Equal, |ordering, (lhs, rhs)| {
                    ordering
                        .then_with(|| lhs.0.cmp(rhs.0))
                        .then_with(|| cmp_value(lhs.1, rhs.1))
                })
        }
        (Value::String(lhs), Value::String(rhs)) => lhs.cmp(rhs),
        _ => Ordering::Equal,
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
