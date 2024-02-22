use crate::lookup::{Branch, Lookup};
use anyhow::{anyhow, Error};
use bson::{Bson, Document};
use serde_json::{Map, Value};
use std::cmp::Ordering;

#[derive(Debug)]
pub struct Sorter {
    lookups: Vec<(Lookup, bool)>,
}

impl Sorter {
    pub fn cmp(&self, lhs: &Map<String, Value>, rhs: &Map<String, Value>) -> Ordering {
        // It simplifies the `max_by`/`min_by` below.
        #[inline(always)]
        fn cmp_value(lhs: &&Value, rhs: &&Value) -> Ordering {
            Sorter::cmp_value(lhs, rhs)
        }

        // TODO: `lookup` requires a `Value`.
        let lhs = Value::Object(lhs.clone());
        let rhs = Value::Object(rhs.clone());

        for (lookup, reverse) in &self.lookups {
            let lhs_values = Branch::expand(lookup.lookup(&lhs), true)
                .into_iter()
                .filter_map(|branch| branch.value);
            let rhs_values = Branch::expand(lookup.lookup(&rhs), true)
                .into_iter()
                .filter_map(|branch| branch.value);

            let (lhs_value, rhs_value) = if *reverse {
                (lhs_values.max_by(cmp_value), rhs_values.max_by(cmp_value))
            } else {
                (lhs_values.min_by(cmp_value), rhs_values.min_by(cmp_value))
            };

            let ordering = Self::cmp_value_option(lhs_value, rhs_value);
            if ordering.is_ne() {
                return if *reverse {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }

        Ordering::Equal
    }

    pub fn cmp_value(lhs: &Value, rhs: &Value) -> Ordering {
        match Self::cmp_value_partial(lhs, rhs) {
            Ok(ordering) | Err(ordering) => ordering,
        }
    }

    pub fn cmp_value_option(lhs: Option<&Value>, rhs: Option<&Value>) -> Ordering {
        match (lhs, rhs) {
            (None, None) => Ordering::Equal,
            (None, _) => Ordering::Less,
            (_, None) => Ordering::Greater,
            (Some(lhs), Some(rhs)) => Self::cmp_value(lhs, rhs),
        }
    }

    pub fn cmp_value_partial(lhs: &Value, rhs: &Value) -> Result<Ordering, Ordering> {
        let lhs_type = Self::value_type(lhs);
        let rhs_type = Self::value_type(rhs);
        if lhs_type != rhs_type {
            let lhs_type_order = Self::value_type_order(lhs_type);
            let rhs_type_order = Self::value_type_order(rhs_type);
            return Err(lhs_type_order.cmp(&rhs_type_order));
        }

        Ok(match (lhs, rhs) {
            (Value::Array(lhs), Value::Array(rhs)) => {
                let ordering = lhs.len().cmp(&rhs.len());
                if ordering.is_ne() {
                    return Ok(ordering);
                }

                for (lhs, rhs) in lhs.iter().zip(rhs.iter()) {
                    let ordering = Self::cmp_value_partial(lhs, rhs)?;
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

                    let ordering = Self::cmp_value_partial(lhs.1, rhs.1)?;
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

    pub fn compile(sort: Option<&Document>) -> Result<Self, Error> {
        let mut dotted_fields = vec![];
        let lookups = sort
            .into_iter()
            .flatten()
            .map(|(key, order)| {
                if let Some((field, _)) = key.split_once('.') {
                    match dotted_fields.binary_search(&field) {
                        Ok(_) => {
                            return Err(anyhow!("Sort for parallel nested fields is not supported"))
                        }
                        Err(index) => dotted_fields.insert(index, field),
                    }
                }

                let reverse = match order {
                    Bson::Int32(1) | Bson::Int64(1) => false,
                    Bson::Int32(-1) | Bson::Int64(-1) => true,
                    order => return Err(anyhow!("Sort order {order} for {key} is not supported")),
                };

                let lookup = Lookup::new(key.to_owned(), true);
                Ok((lookup, reverse))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { lookups })
    }

    // TODO: It coerces all numbers into `1`, just like Meteor. It would be better
    // to support `double`, `int`, `long`, and `decimal` separately.
    /// <https://www.mongodb.com/docs/manual/reference/operator/query/type/#available-types>
    pub fn value_type(value: &Value) -> i8 {
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
                        ["$flags", "$regexp"] => return 11,
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
}

#[cfg(test)]
mod tests {
    use super::Sorter;
    use bson::doc;
    use serde_json::{json, Value};
    use std::cmp::Ordering;

    macro_rules! fail {
        ($name:ident, { $($sort:tt)* }) => {
            #[test]
            fn $name() {
                let sort = doc! { $($sort)* };
                if Sorter::compile(Some(&sort)).is_ok() {
                    panic!("{sort:?} should not be supported");
                }
            }
        };
    }

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

                let sorter = match Sorter::compile(sort) {
                    Ok(sorter) => sorter,
                    Err(error) => panic!("{sort:?} is not supported: {error:?}"),
                };

                assert_eq!(sorter.cmp(&lhs, &lhs), Ordering::Equal);
                assert_eq!(sorter.cmp(&rhs, &rhs), Ordering::Equal);

                assert_eq!(sorter.cmp(&lhs, &rhs), $expected);
                assert_eq!(sorter.cmp(&rhs, &lhs), $expected.reverse());
            }
        };
    }

    macro_rules! eq {($name:ident, { $($sort:tt)* }, { $($lhs:tt)* }, { $($rhs:tt)* }) => {test!($name, { $($sort)* }, { $($lhs)* }, { $($rhs)* }, Ordering::Equal);}}
    macro_rules! lt {($name:ident, { $($sort:tt)* }, { $($lhs:tt)* }, { $($rhs:tt)* }) => {test!($name, { $($sort)* }, { $($lhs)* }, { $($rhs)* }, Ordering::Less);}}

    eq!(empty, {}, {"a": 1}, {"a": 2});

    eq!(simple_1, {"a": 1}, {}, {"a": []});
    lt!(simple_2, {"a": 1}, {"a": []}, {"a": 1});
    lt!(simple_3, {"a": 1}, {"a": 1}, {"a": {}});
    lt!(simple_4, {"a": 1}, {"a": {}}, {"a": true});
    eq!(simple_5, {"a": -1}, {"a": []}, {});
    lt!(simple_6, {"a": -1}, {"a": 1}, {"a": []});
    lt!(simple_7, {"a": -1}, {"a": {}}, {"a": 1});
    lt!(simple_8, {"a": -1}, {"a": true}, {"a": {}});

    lt!(mixed_01, {"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 1, "b": 2});
    lt!(mixed_02, {"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 2, "b": 1});
    lt!(mixed_03, {"a": 1, "b": 1}, {"a": 1, "b": 1}, {"a": 2, "b": 2});
    lt!(mixed_04, {"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 1});
    lt!(mixed_05, {"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 2});
    lt!(mixed_06, {"a": 1, "b": -1}, {"a": 1, "b": 2}, {"a": 1, "b": 1});
    lt!(mixed_07, {"a": 1, "b": -1}, {"a": 1, "b": 1}, {"a": 2, "b": 1});
    lt!(mixed_08, {"a": 1, "b": -1}, {"a": 1, "b": 1}, {"a": 2, "b": 2});
    lt!(mixed_09, {"a": 1, "b": -1}, {"a": 1, "b": 2}, {"a": 2, "b": 1});
    lt!(mixed_10, {"a": 1, "b": -1}, {"a": 1, "b": 2}, {"a": 2, "b": 2});

    lt!(arrays_01, {"a": 1}, {"a": [1, 10, 20]}, {"a": [5, 2, 99]});
    lt!(arrays_02, {"a": -1}, {"a": [5, 2, 99]}, {"a": [1, 10, 20]});
    lt!(arrays_03, {"a.1": 1}, {"a": [5, 2, 99]}, {"a": [1, 10, 20]});
    lt!(arrays_04, {"a.1": -1}, {"a": [1, 10, 20]}, {"a": [5, 2, 99]});
    lt!(arrays_05, {"a": 1}, {"a": [1, [10, 15], 20]}, {"a": [5, [-5, -20], 18]});
    lt!(arrays_06, {"a": -1}, {"a": [1, [10, 15], 20]}, {"a": [5, [-5, -20], 18]});
    lt!(arrays_07, {"a.0": 1}, {"a": [1, [10, 15], 20]}, {"a": [5, [-5, -20], 18]});
    lt!(arrays_08, {"a.0": -1}, {"a": [5, [-5, -20], 18]}, {"a": [1, [10, 15], 20]});
    lt!(arrays_09, {"a.1": 1}, {"a": [5, [-5, -20], 18]}, {"a": [1, [10, 15], 20]});
    lt!(arrays_10, {"a.1": -1}, {"a": [1, [10, 15], 20]}, {"a": [5, [-5, -20], 18]});
    lt!(arrays_11, {"a.1": 1}, {"a": [1, [10, 15], 20]}, {"a": [5, [19, 3], 18]});
    lt!(arrays_12, {"a.1": -1}, {"a": [5, [19, 3], 18]}, {"a": [1, [10, 15], 20]});
    lt!(arrays_13, {"a": 1}, {"a": [1, [10, 15], 20]}, {"a": [5, [19, 3], 18]});
    lt!(arrays_14, {"a": -1}, {"a": [5, [19, 3], 18]}, {"a": [1, [10, 15], 20]});
    lt!(arrays_15, {"a": -1}, {"a": [1, [10, 15], 20]}, {"a": [5, [3, 19], 18]});
    lt!(arrays_16, {"a.0.s": 1}, {"a": [{"s": 1}]}, {"a": [{"s": 2}]});

    fail!(unsupported_1, {"a.x": 1, "a.y": 1});
    fail!(unsupported_2, {"a.b.x": 1, "a.b.y": 1});
}
