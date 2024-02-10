use crate::ejson::into_ejson;
use crate::sorter::cmp_value;
use bson::{Bson, Document};
use serde_json::{Map, Value};
use std::cmp::Ordering;

fn cmp(lhs: &Value, rhs: &Bson) -> Ordering {
    // TODO: This can be done faster by NOT using `into_ejson`.
    cmp_value(lhs, &into_ejson(rhs.clone()))
}

fn is_equal(lhs: &Value, rhs: &Bson) -> bool {
    // TODO: This can be done faster by NOT using `into_ejson`.
    *lhs == into_ejson(rhs.clone())
}

pub fn is_matching(selector: &Document, document: &Map<String, Value>) -> bool {
    selector.iter().all(|(key, selector)| match key.as_ref() {
        key @ ("$and" | "$or") => selector.as_array().is_some_and(|selectors| {
            let mut results = selectors.iter().map(|selector| {
                selector
                    .as_document()
                    .is_some_and(|selector| is_matching(selector, document))
            });

            match key {
                "$and" => results.all(|result| result),
                "$or" => results.any(|result| result),
                _ => unreachable!(),
            }
        }),
        _ => is_matching_value(selector, document.get(key)),
    })
}

fn is_matching_document(selector: &Document, value: &Value) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| is_matching_document(selector, value)),
        Value::Object(value) => is_matching(selector, value),
        _ => false,
    }
}

fn is_matching_value(selector: &Bson, value: Option<&Value>) -> bool {
    match selector {
        Bson::Array(_) => value.is_some_and(|value| value.is_array() && is_equal(value, selector)),
        Bson::Document(selector) => {
            let has_operator = selector.keys().any(|key| key.starts_with('$'));
            if !has_operator {
                return value.is_some_and(|value| is_matching_document(selector, value));
            }

            selector.iter().all(|(key, selector)| match key.as_ref() {
                key @ ("$eq" | "$ne") => {
                    let is_equal = match selector {
                        Bson::Document(_) => value.is_some_and(|value| is_equal(value, selector)),
                        _ => is_matching_value(selector, value),
                    };

                    match key {
                        "$eq" => is_equal,
                        "$ne" => !is_equal,
                        _ => unreachable!(),
                    }
                }
                key @ ("$gt" | "$gte" | "$lt" | "$lte") => {
                    let values = match value {
                        Some(Value::Array(values)) => values.iter().collect(),
                        Some(value) => vec![value],
                        None => vec![],
                    };

                    values.into_iter().any(|value| {
                        let ordering = cmp(value, selector);
                        match key {
                            "$gt" => ordering.is_gt(),
                            "$gte" => ordering.is_ge(),
                            "$lt" => ordering.is_lt(),
                            "$lte" => ordering.is_le(),
                            _ => unreachable!(),
                        }
                    })
                }
                key @ ("$in" | "$nin") => {
                    let is_in = selector.as_array().is_some_and(|selectors| {
                        selectors
                            .iter()
                            .any(|selector| is_matching_value(selector, value))
                    });

                    match key {
                        "$in" => is_in,
                        "$nin" => !is_in,
                        _ => unreachable!(),
                    }
                }
                key => unreachable!("{key}"),
            })
        }
        Bson::Null if value.is_none() => true,
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Null | Bson::String(_) => value
            .is_some_and(|value| match value {
                Value::Array(values) => values
                    .iter()
                    .any(|value| is_matching_value(selector, Some(value))),
                value => is_equal(value, selector),
            }),
        _ => false,
    }
}

pub fn is_supported(selector: &Document) -> bool {
    selector.iter().all(|(key, selector)| {
        // TODO: Implement nested keys.
        if key.contains('.') {
            return false;
        }

        match key.as_ref() {
            "$and" | "$or" => selector
                .as_array()
                .is_some_and(|selectors| selectors.iter().all(is_supported_value)),
            key => !key.starts_with('$') && is_supported_value(selector),
        }
    })
}

fn is_supported_value(selector: &Bson) -> bool {
    match selector {
        Bson::Array(selectors) => selectors.iter().all(is_supported_value),
        Bson::Document(selector) => selector.iter().all(|(key, selector)| match key.as_ref() {
            "$eq" | "$gt" | "$gte" | "$lt" | "$lte" | "$ne" => is_supported_value(selector),
            "$in" | "$nin" => selector
                .as_array()
                .is_some_and(|selectors| selectors.iter().all(is_supported_value)),
            key => !key.starts_with('$') && is_supported_value(selector),
        }),
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Null | Bson::String(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{is_matching, is_supported};
    use bson::doc;
    use serde_json::{json, Value};

    macro_rules! test {
        ($name:ident, { $($selector:tt)* }, { $($document:tt)* }, $expected:expr) => {
            #[test]
            fn $name() {
                let selector = &doc! { $($selector)* };
                let document = json! {{ $($document)* }};

                let Value::Object(document) = document else { unreachable!() };

                assert!(is_supported(selector), "{selector:?} is not supported");
                assert!(
                    is_matching(selector, &document) == $expected,
                    "{selector:?} should{} match {document:?} but does{}",
                    if $expected { "" } else { "n't" },
                    if $expected { "n't" } else { "" },
                );
            }
        };
    }

    macro_rules! y {($name:ident, { $($selector:tt)* }, { $($document:tt)* }) => {test!($name, { $($selector)* }, { $($document)* }, true);}}
    macro_rules! n {($name:ident, { $($selector:tt)* }, { $($document:tt)* }) => {test!($name, { $($selector)* }, { $($document)* }, false);}}

    // Mostly taken from https://github.com/meteor/meteor/blob/7411b3c85a3c95a6b6f3c588babe6eae894d6fb6/packages/minimongo/minimongo_tests_client.js#L384.

    // Empty selector.
    y!(empty_1, {}, {});
    y!(empty_2, {}, {"a": null});

    // Null.
    y!(null_1, {"a": null}, {});
    y!(null_2, {"a": null}, {"a": null});
    y!(null_3, {"a": null}, {"a": [null]});
    n!(null_4, {"a": null}, {"a": 1});

    // Number.
    n!(number_01, {"a": 1}, {});
    n!(number_02, {"a": 1}, {"a": 2});
    y!(number_03, {"a": 1}, {"a": 1});
    n!(number_04, {"a": 1}, {"b": 1});
    n!(number_05, {"a": 1, "b": 2}, {"a": 1});
    n!(number_06, {"a": 1, "b": 2}, {"b": 2});
    n!(number_07, {"a": 1, "b": 2}, {"a": 2, "b": 1});
    y!(number_08, {"a": 1, "b": 2}, {"a": 1, "b": 2});
    n!(number_09, {"a": 1}, {"a": []});
    n!(number_10, {"a": 1}, {"a": ["bar"]});
    y!(number_11, {"a": 1}, {"a": [1]});
    y!(number_12, {"a": 1}, {"a": [1, "bar"]});
    y!(number_13, {"a": 1}, {"a": ["bar", 1]});
    y!(number_14, {"a": 1}, {"a": [1, 1]});

    // String.
    n!(string_01, {"a": "foo"}, {});
    n!(string_02, {"a": "foo"}, {"a": "bar"});
    y!(string_03, {"a": "foo"}, {"a": "foo"});
    n!(string_04, {"a": "foo"}, {"b": "foo"});
    n!(string_05, {"a": "foo", "b": "bar"}, {"a": "foo"});
    n!(string_06, {"a": "foo", "b": "bar"}, {"b": "bar"});
    n!(string_07, {"a": "foo", "b": "bar"}, {"a": "bar", "b": "foo"});
    y!(string_08, {"a": "foo", "b": "bar"}, {"a": "foo", "b": "bar"});
    n!(string_09, {"a": "foo"}, {"a": []});
    n!(string_10, {"a": "foo"}, {"a": ["bar"]});
    y!(string_11, {"a": "foo"}, {"a": ["foo"]});
    y!(string_12, {"a": "foo"}, {"a": ["foo", "bar"]});
    y!(string_13, {"a": "foo"}, {"a": ["bar", "foo"]});
    y!(string_14, {"a": "foo"}, {"a": ["foo", "foo"]});

    // $and.
    y!(operator_and_1, {"$and": [{"a": 1}]}, {"a": 1});
    n!(operator_and_2, {"$and": [{"a": 1}, {"a": 2}]}, {"a": 1});
    n!(operator_and_3, {"$and": [{"a": 1}, {"b": 1}]}, {"a": 1});
    y!(operator_and_4, {"$and": [{"a": 1}, {"b": 2}]}, {"a": 1, "b": 2});
    n!(operator_and_5, {"$and": [{"a": 1}, {"b": 1}]}, {"a": 1, "b": 2});
    y!(operator_and_6, {"$and": [{"a": 1}, {"b": 2}], "c": 3}, {"a": 1, "b": 2, "c": 3});
    n!(operator_and_7, {"$and": [{"a": 1}, {"b": 2}], "c": 4}, {"a": 1, "b": 2, "c": 3});

    // $gt.
    y!(operator_gt_1, {"a": {"$gt": 10}}, {"a": 11});
    n!(operator_gt_2, {"a": {"$gt": 10}}, {"a": 10});
    n!(operator_gt_3, {"a": {"$gt": 10}}, {"a": 9});
    y!(operator_gt_4, {"a": {"$gt": {"x": [2, 3, 4]}}}, {"a": {"x": [3, 3, 4]}});
    n!(operator_gt_5, {"a": {"$gt": {"x": [2, 3, 4]}}}, {"a": {"x": [1, 3, 4]}});
    n!(operator_gt_6, {"a": {"$gt": {"x": [2, 3, 4]}}}, {"a": {"x": [2, 3, 4]}});
    n!(operator_gt_7, {"a": {"$gt": [2, 3]}}, {"a": [1, 2]});

    // $gte.
    y!(operator_gte_1, {"a": {"$gte": 10}}, {"a": 11});
    y!(operator_gte_2, {"a": {"$gte": 10}}, {"a": 10});
    n!(operator_gte_3, {"a": {"$gte": 10}}, {"a": 9});
    y!(operator_gte_4, {"a": {"$gte": {"x": [2, 3, 4]}}}, {"a": {"x": [2, 3, 4]}});

    // $in.
    y!(operator_in_01, {"a": {"$in": [1, 2, 3]}}, {"a": 2});
    n!(operator_in_02, {"a": {"$in": [1, 2, 3]}}, {"a": 4});
    y!(operator_in_03, {"a": {"$in": [[1], [2], [3]]}}, {"a": [2]});
    n!(operator_in_04, {"a": {"$in": [[1], [2], [3]]}}, {"a": [4]});
    y!(operator_in_05, {"a": {"$in": [{"b": 1}, {"b": 2}, {"b": 3}]}}, {"a": {"b": 2}});
    n!(operator_in_06, {"a": {"$in": [{"b": 1}, {"b": 2}, {"b": 3}]}}, {"a": {"b": 4}});
    y!(operator_in_07, {"a": {"$in": [1, 2, 3]}}, {"a": [2]});
    y!(operator_in_08, {"a": {"$in": [{"b": 1}, {"b": 2}, {"b": 3}]}}, {"a": [{"b": 2}]});
    y!(operator_in_09, {"a": {"$in": [1, 2, 3]}}, {"a": [4, 2]});
    n!(operator_in_10, {"a": {"$in": [1, 2, 3]}}, {"a": [4]});
    y!(operator_in_11, {"a": {"$in": [1, null]}}, {});
    y!(operator_in_12, {"a": {"$in": [1, null]}}, {"a": null});

    // $eq.
    n!(operator_eq_01, {"a": {"$eq": 1}}, {"a": 2});
    y!(operator_eq_02, {"a": {"$eq": 2}}, {"a": 2});
    n!(operator_eq_03, {"a": {"$eq": [1]}}, {"a": [2]});
    y!(operator_eq_04, {"a": {"$eq": [1, 2]}}, {"a": [1, 2]});
    y!(operator_eq_05, {"a": {"$eq": 1}}, {"a": [1, 2]});
    y!(operator_eq_06, {"a": {"$eq": 2}}, {"a": [1, 2]});
    n!(operator_eq_07, {"a": {"$eq": 3}}, {"a": [1, 2]});
    y!(operator_eq_08, {"a": {"$eq": {"x": 1}}}, {"a": {"x": 1}});
    n!(operator_eq_09, {"a": {"$eq": {"x": 1}}}, {"a": {"x": 2}});
    n!(operator_eq_10, {"a": {"$eq": {"x": 1}}}, {"a": {"x": 1, "y": 2}});

    // $lt.
    y!(operator_lt_1, {"a": {"$lt": 10}}, {"a": 9});
    n!(operator_lt_2, {"a": {"$lt": 10}}, {"a": 10});
    n!(operator_lt_3, {"a": {"$lt": 10}}, {"a": 11});
    y!(operator_lt_4, {"a": {"$lt": 10}}, {"a": [11, 9, 12]});
    n!(operator_lt_5, {"a": {"$lt": 10}}, {"a": [11, 12]});
    n!(operator_lt_6, {"a": {"$lt": "null"}}, {"a": null});
    y!(operator_lt_7, {"a": {"$lt": {"x": [2, 3, 4]}}}, {"a": {"x": [1, 3, 4]}});
    n!(operator_lt_8, {"a": {"$lt": {"x": [2, 3, 4]}}}, {"a": {"x": [2, 3, 4]}});

    // $lte.
    y!(operator_lte_1, {"a": {"$lte": 10}}, {"a": 9});
    y!(operator_lte_2, {"a": {"$lte": 10}}, {"a": 10});
    n!(operator_lte_3, {"a": {"$lte": 10}}, {"a": 11});
    y!(operator_lte_4, {"a": {"$lte": {"x": [2, 3, 4]}}}, {"a": {"x": [2, 3, 4]}});

    // $ne.
    y!(operator_ne_01, {"a": {"$ne": 1}}, {"a": 2});
    n!(operator_ne_02, {"a": {"$ne": 2}}, {"a": 2});
    y!(operator_ne_03, {"a": {"$ne": [1]}}, {"a": [2]});
    n!(operator_ne_04, {"a": {"$ne": [1, 2]}}, {"a": [1, 2]});
    n!(operator_ne_05, {"a": {"$ne": 1}}, {"a": [1, 2]});
    n!(operator_ne_06, {"a": {"$ne": 2}}, {"a": [1, 2]});
    y!(operator_ne_07, {"a": {"$ne": 3}}, {"a": [1, 2]});
    n!(operator_ne_08, {"a": {"$ne": {"x": 1}}}, {"a": {"x": 1}});
    y!(operator_ne_09, {"a": {"$ne": {"x": 1}}}, {"a": {"x": 2}});
    y!(operator_ne_10, {"a": {"$ne": {"x": 1}}}, {"a": {"x": 1, "y": 2}});

    // $nin.
    n!(operator_nin_01, {"a": {"$nin": [1, 2, 3]}}, {"a": 2});
    y!(operator_nin_02, {"a": {"$nin": [1, 2, 3]}}, {"a": 4});
    n!(operator_nin_03, {"a": {"$nin": [[1], [2], [3]]}}, {"a": [2]});
    y!(operator_nin_04, {"a": {"$nin": [[1], [2], [3]]}}, {"a": [4]});
    n!(operator_nin_05, {"a": {"$nin": [{"b": 1}, {"b": 2}, {"b": 3}]}}, {"a": {"b": 2}});
    y!(operator_nin_06, {"a": {"$nin": [{"b": 1}, {"b": 2}, {"b": 3}]}}, {"a": {"b": 4}});
    n!(operator_nin_07, {"a": {"$nin": [1, 2, 3]}}, {"a": [2]});
    n!(operator_nin_08, {"a": {"$nin": [{"b": 1}, {"b": 2}, {"b": 3}]}}, {"a": [{"b": 2}]});
    n!(operator_nin_09, {"a": {"$nin": [1, 2, 3]}}, {"a": [4, 2]});
    y!(operator_nin_10, {"a": {"$nin": [1, 2, 3]}}, {"a": [4]});
    n!(operator_nin_11, {"a": {"$nin": [1, null]}}, {});
    n!(operator_nin_12, {"a": {"$nin": [1, null]}}, {"a": null});

    // $or.
    y!(operator_or_01, {"$or": [{"a": 1}]}, {"a": 1});
    n!(operator_or_02, {"$or": [{"b": 2}]}, {"a": 1});
    y!(operator_or_03, {"$or": [{"a": 1}, {"b": 2}]}, {"a": 1});
    n!(operator_or_04, {"$or": [{"c": 3}, {"d": 4}]}, {"a": 1});
    y!(operator_or_05, {"$or": [{"a": 1}, {"b": 2}]}, {"a": [1, 2, 3]});
    n!(operator_or_06, {"$or": [{"a": 1}, {"b": 2}]}, {"c": [1, 2, 3]});
    n!(operator_or_07, {"$or": [{"a": 1}, {"b": 2}]}, {"a": [2, 3, 4]});
    y!(operator_or_08, {"$or": [{"a": 1}, {"a": 2}]}, {"a": 1});
    y!(operator_or_09, {"$or": [{"a": 1}, {"a": 2}], "b": 2}, {"a": 1, "b": 2});
    n!(operator_or_10, {"$or": [{"a": 2}, {"a": 3}], "b": 2}, {"a": 1, "b": 2});
    n!(operator_or_11, {"$or": [{"a": 1}, {"a": 2}], "b": 3}, {"a": 1, "b": 2});
    y!(operator_or_12, {"x": 1, "$or": [{"a": 1}, {"b": 1}]}, {"x": 1, "b": 1});
    y!(operator_or_13, {"$or": [{"a": 1}, {"b": 1}], "x": 1}, {"x": 1, "b": 1});
    n!(operator_or_14, {"x": 1, "$or": [{"a": 1}, {"b": 1}]}, {"b": 1});
    n!(operator_or_15, {"x": 1, "$or": [{"a": 1}, {"b": 1}]}, {"x": 1});
    y!(operator_or_16, {"$or": [{"a": {"b": 1, "c": 2}}, {"a": {"b": 2, "c": 1}}]}, {"a": {"b": 1, "c": 2}});
    n!(operator_or_17, {"$or": [{"a": {"b": 1, "c": 3}}, {"a": {"b": 2, "c": 1}}]}, {"a": {"b": 1, "c": 2}});

    // $and + $in.
    n!(operators_and_in_1, {"$and": [{"a": {"$in": []}}]}, {});
    y!(operators_and_in_2, {"$and": [{"a": {"$in": [1, 2, 3]}}]}, {"a": 1});
    n!(operators_and_in_3, {"$and": [{"a": {"$in": [4, 5, 6]}}]}, {"a": 1});
    n!(operators_and_in_4, {"$and": [{"a": {"$in": [1, 2, 3]}}, {"a": {"$in": [4, 5, 6]}}]}, {"a": 1});
    n!(operators_and_in_5, {"$and": [{"a": {"$in": [1, 2, 3]}}, {"b": {"$in": [1, 2, 3]}}]}, {"a": 1, "b": 4});
    y!(operators_and_in_6, {"$and": [{"a": {"$in": [1, 2, 3]}}, {"b": {"$in": [4, 5, 6]}}]}, {"a": 1, "b": 4});

    // $and + $ne.
    y!(operators_and_ne_1, {"$and": [{"a": {"$ne": 1}}]}, {});
    n!(operators_and_ne_2, {"$and": [{"a": {"$ne": 1}}]}, {"a": 1});
    y!(operators_and_ne_3, {"$and": [{"a": {"$ne": 1}}]}, {"a": 2});
    n!(operators_and_ne_4, {"$and": [{"a": {"$ne": 1}}, {"a": {"$ne": 2}}]}, {"a": 2});
    y!(operators_and_ne_5, {"$and": [{"a": {"$ne": 1}}, {"a": {"$ne": 3}}]}, {"a": 2});

    // $gt + $lt.
    n!(operator_gt_lt_1, {"a": {"$lt": 11, "$gt": 9}}, {"a": 8});
    n!(operator_gt_lt_2, {"a": {"$lt": 11, "$gt": 9}}, {"a": 9});
    y!(operator_gt_lt_3, {"a": {"$lt": 11, "$gt": 9}}, {"a": 10});
    n!(operator_gt_lt_4, {"a": {"$lt": 11, "$gt": 9}}, {"a": 11});
    n!(operator_gt_lt_5, {"a": {"$lt": 11, "$gt": 9}}, {"a": 12});
    y!(operator_gt_lt_6, {"a": {"$lt": 11, "$gt": 9}}, {"a": [8, 9, 10, 11, 12]});
    y!(operator_gt_lt_7, {"a": {"$lt": 11, "$gt": 9}}, {"a": [8, 9, 11, 12]});

    // $in + $or.
    y!(operators_in_or_1, {"$or": [{"a": {"$in": [1, 2, 3]}}]}, {"a": 1});
    n!(operators_in_or_2, {"$or": [{"a": {"$in": [4, 5, 6]}}]}, {"a": 1});
    y!(operators_in_or_3, {"$or": [{"a": {"$in": [1, 2, 3]}}, {"b": 2}]}, {"a": 1});
    y!(operators_in_or_4, {"$or": [{"a": {"$in": [1, 2, 3]}}, {"b": 2}]}, {"b": 2});
    n!(operators_in_or_5, {"$or": [{"a": {"$in": [1, 2, 3]}}, {"b": 2}]}, {"c": 3});
    y!(operators_in_or_6, {"$or": [{"a": {"$in": [1, 2, 3]}}, {"b": {"$in": [1, 2, 3]}}]}, {"b": 2});
    n!(operators_in_or_7, {"$or": [{"a": {"$in": [1, 2, 3]}}, {"b": {"$in": [4, 5, 6]}}]}, {"b": 2});

    // $ne + $or.
    y!(operators_ne_or_1, {"$or": [{"a": {"$ne": 1}}]}, {});
    n!(operators_ne_or_2, {"$or": [{"a": {"$ne": 1}}]}, {"a": 1});
    y!(operators_ne_or_3, {"$or": [{"a": {"$ne": 1}}]}, {"a": 2});
    y!(operators_ne_or_4, {"$or": [{"a": {"$ne": 1}}]}, {"b": 1});
    y!(operators_ne_or_5, {"$or": [{"a": {"$ne": 1}}, {"a": {"$ne": 2}}]}, {"a": 1});
    y!(operators_ne_or_6, {"$or": [{"a": {"$ne": 1}}, {"b": {"$ne": 1}}]}, {"a": 1});
    n!(operators_ne_or_7, {"$or": [{"a": {"$ne": 1}}, {"b": {"$ne": 2}}]}, {"a": 1, "b": 2});
}
