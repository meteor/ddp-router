use crate::ejson::into_ejson;
use bson::{Bson, Document};
use serde_json::{Map, Value};

pub fn is_matching(selector: &Document, document: &Map<String, Value>) -> bool {
    selector.iter().all(|(key, selector)| {
        // TODO: Implement operators.
        is_matching_value(selector, document.get(key))
    })
}

fn is_matching_value(selector: &Bson, value: Option<&Value>) -> bool {
    match selector {
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Null | Bson::String(_) => {
            // TODO: This can be done faster by NOT using `into_ejson`.
            value.is_some_and(|rhs| *rhs == into_ejson(selector.clone()))
        }
        Bson::Document(selector) => {
            let mut keys: Vec<_> = selector.keys().map(String::as_str).collect();
            keys.sort_unstable();
            match keys.as_slice() {
                ["$in"] => selector.get_array("$in").is_ok_and(|selectors| {
                    selectors
                        .iter()
                        .any(|selector| is_matching_value(selector, value))
                }),
                _ => false,
            }
        }
        _ => false,
    }
}

pub fn is_supported(selector: &Document) -> bool {
    selector.iter().all(|(key, selector)| {
        // TODO: Implement nested keys.
        if key.contains('.') {
            return false;
        }

        // TODO: Implement operators.
        if key.starts_with('$') {
            return false;
        }

        is_supported_value(selector)
    })
}

fn is_supported_value(selector: &Bson) -> bool {
    match selector {
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Null | Bson::String(_) => true,
        Bson::Document(selector) => {
            let mut keys: Vec<_> = selector.keys().map(String::as_str).collect();
            keys.sort_unstable();
            match keys.as_slice() {
                // TODO: Implement other value selectors.
                ["$in"] => selector
                    .get_array("$in")
                    .is_ok_and(|selectors| selectors.iter().all(is_supported_value)),
                _ => false,
            }
        }
        // TODO: Implement other selectors.
        _ => false,
    }
}
