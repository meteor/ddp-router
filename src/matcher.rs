use crate::ejson::into_ejson;
use bson::{Bson, Document};
use serde_json::{Map, Value};

fn is_equal(lhs: &Bson, rhs: &Value) -> bool {
    // TODO: This can be done faster by NOT using `into_ejson`.
    into_ejson(lhs.clone()) == *rhs
}

pub fn is_matching(selector: &Document, document: &Map<String, Value>) -> bool {
    selector.iter().all(|(key, selector)| {
        match key.as_ref() {
            "$and" => selector.as_array().is_some_and(|selectors| {
                selectors.iter().all(|selector| {
                    selector
                        .as_document()
                        .is_some_and(|selector| is_matching(selector, document))
                })
            }),
            "$or" => selector.as_array().is_some_and(|selectors| {
                selectors.iter().any(|selector| {
                    selector
                        .as_document()
                        .is_some_and(|selector| is_matching(selector, document))
                })
            }),
            // TODO: Implement other operators.
            _ => is_matching_value(selector, document.get(key)),
        }
    })
}

fn is_matching_document(selector: &Document, value: &Value) -> bool {
    match value {
        Value::Array(value) => value
            .iter()
            .any(|value| is_matching_document(selector, value)),
        Value::Object(value) => is_matching(selector, value),
        _ => false,
    }
}

fn is_matching_value(selector: &Bson, value: Option<&Value>) -> bool {
    match selector {
        Bson::Array(_) => value.is_some_and(|value| value.is_array() && is_equal(selector, value)),
        Bson::Document(selector) => match keys(selector).as_slice() {
            ["$eq"] => selector.get("$eq").is_some_and(|selector| match selector {
                Bson::Document(_) => value.is_some_and(|value| is_equal(selector, value)),
                _ => is_matching_value(selector, value),
            }),
            ["$in"] => selector.get_array("$in").is_ok_and(|selectors| {
                selectors
                    .iter()
                    .any(|selector| is_matching_value(selector, value))
            }),
            ["$ne"] => selector.get("$ne").is_some_and(|selector| match selector {
                Bson::Document(_) => value.map_or(true, |value| !is_equal(selector, value)),
                _ => !is_matching_value(selector, value),
            }),
            // TODO: Implement other operators.
            _ => value.is_some_and(|value| is_matching_document(selector, value)),
        },
        Bson::Null if value.is_none() => true,
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Null | Bson::String(_) => value
            .is_some_and(|value| match value {
                Value::Array(value) => value
                    .iter()
                    .any(|value| is_matching_value(selector, Some(value))),
                value => is_equal(selector, value),
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

        // TODO: Implement other operators.
        if key.starts_with('$') {
            return matches!(key.as_ref(), "$and" | "$or");
        }

        is_supported_value(selector)
    })
}

fn is_supported_value(selector: &Bson) -> bool {
    match selector {
        Bson::Array(selectors) => selectors.iter().all(is_supported_value),
        Bson::Document(selector) => {
            match keys(selector).as_slice() {
                ["$eq"] => selector.get("$eq").is_some_and(is_supported_value),
                ["$in"] => selector
                    .get_array("$in")
                    .is_ok_and(|selectors| selectors.iter().all(is_supported_value)),
                ["$ne"] => selector.get("$ne").is_some_and(is_supported_value),
                // TODO: Implement other value selectors.
                _ => is_supported(selector),
            }
        }
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Null | Bson::String(_) => true,
        // TODO: Implement other selectors.
        _ => false,
    }
}

fn keys(document: &Document) -> Vec<&str> {
    let mut keys: Vec<_> = document.keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys
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
