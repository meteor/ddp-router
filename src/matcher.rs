use std::cmp::Ordering;

use crate::ejson::into_ejson;
use crate::sorter::{cmp_value, cmp_value_partial};
use anyhow::{anyhow, Error};
use bson::{Bson, Document};
use serde_json::{Map, Value};

#[derive(Debug)]
pub enum DocumentMatcher {
    All(Vec<DocumentMatcher>),
    Any(Vec<DocumentMatcher>),
    Lookup {
        #[allow(private_interfaces)]
        lookup: ValueLookup,
        #[allow(private_interfaces)]
        matcher: BranchedMatcher,
    },
}

impl DocumentMatcher {
    fn all(matchers: Vec<Self>) -> Self {
        one_or_wrap(matchers, Self::All)
    }

    fn any(matchers: Vec<Self>) -> Self {
        one_or_wrap(matchers, Self::Any)
    }

    pub fn compile(selector: &Document) -> Result<Self, Error> {
        Self::compile_inner(selector, false, true)
    }

    fn compile_inner(
        selector: &Document,
        is_in_elem_match: bool,
        is_root: bool,
    ) -> Result<Self, Error> {
        Ok(Self::all(
            selector
                .iter()
                .map(|(key, sub_selector)| {
                    if key.starts_with('$') {
                        Self::compile_logical_operator(key, sub_selector, is_in_elem_match)
                    } else {
                        Ok(Self::Lookup {
                            lookup: ValueLookup::new(key.to_owned(), false),
                            matcher: BranchedMatcher::compile_value_selector(
                                sub_selector,
                                is_root,
                            )?,
                        })
                    }
                })
                .collect::<Result<_, _>>()?,
        ))
    }

    fn compile_logical_operator(
        operator: &str,
        selector: &Bson,
        is_in_elem_match: bool,
    ) -> Result<Self, Error> {
        match operator {
            "$and" => Self::compile_many(selector, is_in_elem_match).map(Self::all),
            "$or" => Self::compile_many(selector, is_in_elem_match).map(Self::any),
            operator => Err(anyhow!("{operator} is not supported")),
        }
    }

    fn compile_many(selector: &Bson, is_in_elem_match: bool) -> Result<Vec<Self>, Error> {
        selector
            .as_array()
            .ok_or_else(|| anyhow!("Expected array of selectors, got {selector:?}"))?
            .iter()
            .map(|selector| {
                let document = selector
                    .as_document()
                    .ok_or_else(|| anyhow!("Expected document selector, got {selector:?}"))?;
                Self::compile_inner(document, is_in_elem_match, false)
            })
            .collect()
    }

    pub fn matches(&self, document: &Map<String, Value>) -> bool {
        match &self {
            Self::All(matchers) => matchers.iter().all(|matcher| matcher.matches(document)),
            Self::Any(matchers) => matchers.iter().any(|matcher| matcher.matches(document)),
            Self::Lookup { lookup, matcher } => {
                // TODO: Get rid of `clone`.
                matcher.matches(lookup.lookup(&Value::Object(document.clone())))
            }
        }
    }
}

#[derive(Debug)]
enum BranchedMatcher {
    All(Vec<BranchedMatcher>),
    Any(Vec<BranchedMatcher>),
    Element {
        matcher: ElementMatcher,
        dont_expand_leaf_arrays: bool,
        dont_include_leaf_arrays: bool,
    },
    Invert(Box<BranchedMatcher>),
}

impl BranchedMatcher {
    fn all(matchers: Vec<Self>) -> Self {
        one_or_wrap(matchers, Self::All)
    }

    fn any(matchers: Vec<Self>) -> Self {
        one_or_wrap(matchers, Self::Any)
    }

    fn compile_operator(
        operator: &str,
        operand: &Bson,
        _selector: &Bson,
        _is_root: bool,
    ) -> Result<Self, Error> {
        match operator {
            "$eq" => Ok(ElementMatcher::compile(operand).into_branched(false, false)),
            "$exists" => {
                let matcher = ElementMatcher::Exists.into_branched(false, false);
                Ok(match operand {
                    Bson::Boolean(true) => matcher,
                    _ => matcher.invert(),
                })
            }
            "$gt" | "$gte" | "$lt" | "$lte" => {
                let (ordering, is_negated) = match operator {
                    "$gt" => (Ordering::Greater, false),
                    "$gte" => (Ordering::Less, true),
                    "$lt" => (Ordering::Less, false),
                    "$lte" => (Ordering::Greater, true),
                    _ => unreachable!(),
                };

                Ok(ElementMatcher::Order {
                    selector: into_ejson(operand.clone()),
                    ordering,
                    is_negated,
                }
                .into_branched(false, false))
            }
            "$in" => Ok(Self::any(
                operand
                    .as_array()
                    .ok_or_else(|| anyhow!("$in expected an array, got {operand:?}"))?
                    .iter()
                    .map(|operand| {
                        if is_operator_object(operand) {
                            Err(anyhow!("$in expected plain document, got {operand:?}"))
                        } else {
                            Ok(ElementMatcher::compile(operand).into_branched(false, false))
                        }
                    })
                    .collect::<Result<_, _>>()?,
            )),
            "$ne" => Self::compile_operator("$eq", operand, _selector, _is_root).map(Self::invert),
            "$nin" => Self::compile_operator("$in", operand, _selector, _is_root).map(Self::invert),
            "$size" => {
                let size: usize = match operand {
                    // TODO: Can we make it safe?
                    Bson::Int32(size) if *size >= 0 => (*size).try_into().unwrap(),
                    Bson::Int64(size) if *size >= 0 => (*size).try_into().unwrap(),
                    operand => {
                        return Err(anyhow!(
                            "$size expected a non-negative number, got {operand:?}"
                        ))
                    }
                };

                Ok(ElementMatcher::Size(size).into_branched(true, false))
            }
            operator => Err(anyhow!("{operator} is not supported")),
        }
    }

    fn compile_value_selector(selector: &Bson, is_root: bool) -> Result<Self, Error> {
        if is_operator_object(selector) {
            Ok(Self::all(
                selector
                    .as_document()
                    .ok_or_else(|| anyhow!(", got {selector:?}"))?
                    .iter()
                    .map(|(operator, operand)| {
                        Self::compile_operator(operator, operand, selector, is_root)
                    })
                    .collect::<Result<_, _>>()?,
            ))
        } else {
            Ok(ElementMatcher::compile(selector).into_branched(false, false))
        }
    }

    fn invert(self) -> Self {
        Self::Invert(Box::new(self))
    }

    fn matches(&self, branches: Vec<ValueBranch>) -> bool {
        match &self {
            Self::All(matchers) => matchers
                .iter()
                .all(|matcher| matcher.matches(branches.clone())),
            Self::Any(matchers) => matchers
                .iter()
                .any(|matcher| matcher.matches(branches.clone())),
            Self::Element {
                matcher,
                dont_expand_leaf_arrays,
                dont_include_leaf_arrays,
            } => {
                let mut expanded = branches;
                if !*dont_expand_leaf_arrays {
                    expanded = ValueBranch::expand(expanded, *dont_include_leaf_arrays);
                }

                expanded
                    .into_iter()
                    .any(|element| matcher.matches(element.value))
            }
            Self::Invert(matcher) => !matcher.matches(branches),
        }
    }
}

#[derive(Debug)]
enum ElementMatcher {
    Exists,
    Order {
        selector: Value,
        ordering: Ordering,
        is_negated: bool,
    },
    Size(usize),
    Value(Value),
}

impl ElementMatcher {
    fn compile(selector: &Bson) -> Self {
        Self::Value(into_ejson(selector.clone()))
    }

    fn into_branched(
        self,
        dont_expand_leaf_arrays: bool,
        dont_include_leaf_arrays: bool,
    ) -> BranchedMatcher {
        BranchedMatcher::Element {
            matcher: self,
            dont_expand_leaf_arrays,
            dont_include_leaf_arrays,
        }
    }

    fn matches(&self, maybe_value: Option<&Value>) -> bool {
        match &self {
            Self::Exists => maybe_value.is_some(),
            Self::Order {
                selector: Value::Array(_),
                ..
            } => false,
            Self::Order {
                selector,
                ordering,
                is_negated,
            } => {
                let value = maybe_value.unwrap_or(&Value::Null);
                let result = cmp_value_partial(value, selector);
                result.is_ok_and(|result| result == *ordering) != *is_negated
            }
            Self::Size(size) => maybe_value
                .and_then(Value::as_array)
                .is_some_and(|array| array.len() == *size),
            Self::Value(Value::Null) => maybe_value.map_or(true, Value::is_null),
            Self::Value(selector) => {
                maybe_value.is_some_and(|value| cmp_value(selector, value).is_eq())
            }
        }
    }
}

#[derive(Clone, Debug)]
struct ValueBranch<'a> {
    dont_iterate: bool,
    value: Option<&'a Value>,
}

impl ValueBranch<'_> {
    fn expand(branches: Vec<Self>, skip_the_arrays: bool) -> Vec<Self> {
        let mut branches_out = vec![];
        for branch in branches {
            let this_is_array = branch.value.is_some_and(Value::is_array);
            if !(skip_the_arrays && this_is_array && !branch.dont_iterate) {
                branches_out.push(branch.clone());
            }

            if this_is_array && !branch.dont_iterate {
                for value in branch.value.unwrap().as_array().unwrap() {
                    branches_out.push(Self {
                        value: Some(value),
                        dont_iterate: false,
                    });
                }
            }
        }

        branches_out
    }
}

#[derive(Debug)]
struct ValueLookup {
    for_sort: bool,
    key_head: String,
    key_head_usize: Option<usize>,
    key_next_usize: bool,
    lookup_rest: Option<Box<ValueLookup>>,
}

impl ValueLookup {
    fn lookup<'a>(&'a self, value: &'a Value) -> Vec<ValueBranch> {
        let Self {
            for_sort,
            key_head,
            key_head_usize,
            key_next_usize,
            lookup_rest,
        } = &self;

        if let Value::Array(values) = value {
            if !key_head_usize.is_some_and(|index| index < values.len()) {
                return vec![];
            }
        }

        let value_head = match value {
            Value::Array(values) => key_head_usize.and_then(|index| values.get(index)),
            Value::Object(values) => values.get(key_head),
            _ => None,
        };

        let Some(lookup_rest) = lookup_rest else {
            return vec![ValueBranch {
                value: value_head,
                dont_iterate: value.is_array() && value_head.is_some_and(Value::is_array),
            }];
        };

        let Some(value_head) = value_head.filter(|value_head| is_indexable(value_head)) else {
            return if value.is_array() {
                vec![]
            } else {
                vec![ValueBranch {
                    value: None,
                    dont_iterate: false,
                }]
            };
        };

        let mut result = lookup_rest.lookup(value_head);

        if !(*key_next_usize && *for_sort) {
            if let Value::Array(branches) = value_head {
                for branch in branches {
                    // TODO: Exclude BSON-specific types.
                    if branch.is_object() {
                        result.extend(lookup_rest.lookup(branch));
                    }
                }
            }
        }

        result
    }

    fn new(key: String, for_sort: bool) -> Self {
        let (key_head, key_next_usize, lookup_rest) = match key.split_once('.') {
            Some((key_head, key_rest)) => (
                key_head.to_owned(),
                key_rest
                    .split_once('.')
                    .map_or(key_rest, |(key_next, _)| key_next)
                    .parse::<usize>()
                    .is_ok(),
                Some(Box::new(Self::new(key_rest.to_owned(), for_sort))),
            ),
            None => (key, false, None),
        };
        let key_head_usize = key_head.parse::<usize>().ok();

        Self {
            for_sort,
            key_head,
            key_head_usize,
            key_next_usize,
            lookup_rest,
        }
    }
}

fn is_indexable(value: &Value) -> bool {
    matches!(value, Value::Array(_) | Value::Object(_))
}

fn is_operator_object(selector: &Bson) -> bool {
    selector.as_document().is_some_and(|selector| {
        selector
            .keys()
            .next()
            .is_some_and(|key| key.starts_with('$'))
    })
}

fn one_or_wrap<T>(mut list: Vec<T>, wrap: impl Fn(Vec<T>) -> T) -> T {
    match list.len() {
        1 => list.swap_remove(0),
        _ => wrap(list),
    }
}

#[cfg(test)]
mod tests {
    use super::DocumentMatcher;
    use crate::ejson::into_ejson_document;
    use bson::{doc, DateTime};

    macro_rules! test {
        ($name:ident, { $($selector:tt)* }, { $($document:tt)* }, $expected:expr) => {
            #[test]
            fn $name() {
                let selector = &doc! { $($selector)* };
                let document = &into_ejson_document(doc! { $($document)* });

                let matcher = match DocumentMatcher::compile(selector) {
                    Ok(matcher) => matcher,
                    Err(error) => panic!("{selector:?} is not supported: {error:?}"),
                };

                assert!(
                    matcher.matches(document) == $expected,
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

    // Boolean.
    n!(boolean_1, {"a": true}, {});
    n!(boolean_2, {"a": false}, {});
    y!(boolean_3, {"a": true}, {"a": true});
    n!(boolean_4, {"a": false}, {"a": true});
    n!(boolean_5, {"a": true}, {"a": false});
    y!(boolean_6, {"a": false}, {"a": false});

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

    // Date.
    y!(date_01, {"a": DateTime::MIN}, {"a": DateTime::MIN});
    n!(date_02, {"a": DateTime::MIN}, {"a": DateTime::MAX});
    n!(date_03, {"a": DateTime::MIN}, {"a": DateTime::from_millis(0)});
    n!(date_04, {"a": DateTime::MAX}, {"a": DateTime::MIN});
    y!(date_05, {"a": DateTime::MAX}, {"a": DateTime::MAX});
    n!(date_06, {"a": DateTime::MAX}, {"a": DateTime::from_millis(0)});
    n!(date_07, {"a": DateTime::from_millis(0)}, {"a": DateTime::MIN});
    n!(date_08, {"a": DateTime::from_millis(0)}, {"a": DateTime::MAX});
    y!(date_09, {"a": DateTime::from_millis(0)}, {"a": DateTime::from_millis(0)});
    n!(date_10, {"a": {"$gt": DateTime::from_millis(0)}}, {"a": DateTime::MIN});
    n!(date_11, {"a": {"$gte": DateTime::from_millis(0)}}, {"a": DateTime::MIN});
    y!(date_12, {"a": {"$lt": DateTime::from_millis(0)}}, {"a": DateTime::MIN});
    y!(date_13, {"a": {"$lte": DateTime::from_millis(0)}}, {"a": DateTime::MIN});
    y!(date_14, {"a": {"$gt": DateTime::from_millis(0)}}, {"a": DateTime::MAX});
    y!(date_15, {"a": {"$gte": DateTime::from_millis(0)}}, {"a": DateTime::MAX});
    n!(date_16, {"a": {"$lt": DateTime::from_millis(0)}}, {"a": DateTime::MAX});
    n!(date_17, {"a": {"$lte": DateTime::from_millis(0)}}, {"a": DateTime::MAX});

    // Nested paths.
    y!(nested_01, {"a.b": 1}, {"a": {"b": 1}});
    n!(nested_02, {"a.b": 1}, {"a": {"b": 2}});
    y!(nested_03, {"a.b": [1, 2, 3]}, {"a": {"b": [1, 2, 3]}});
    n!(nested_04, {"a.b": [1, 2, 3]}, {"a": {"b": [4]}});
    y!(nested_05, {"a.b.c": null}, {});
    y!(nested_06, {"a.b.c": null}, {"a": 1});
    y!(nested_07, {"a.b": null}, {"a": 1});
    y!(nested_08, {"a.b.c": null}, {"a": {"b": 4}});
    n!(nested_09, {"a.b": null}, {"a": [1]});
    y!(nested_10, {"a.b": []}, {"a": {"b": []}});
    y!(nested_11, {"a.b": 1}, {"a": [{"b": 1}, 2, {}, {"b": [3, 4]}]});
    y!(nested_12, {"a.b": [3, 4]}, {"a": [{"b": 1}, 2, {}, {"b": [3, 4]}]});
    y!(nested_13, {"a.b": 3}, {"a": [{"b": 1}, 2, {}, {"b": [3, 4]}]});
    y!(nested_14, {"a.b": 4}, {"a": [{"b": 1}, 2, {}, {"b": [3, 4]}]});
    y!(nested_15, {"a.b": null}, {"a": [{"b": 1}, 2, {}, {"b": [3, 4]}]});
    y!(nested_16, {"a.1": 8}, {"a": [7, 8, 9]});
    n!(nested_17, {"a.1": 7}, {"a": [7, 8, 9]});
    n!(nested_18, {"a.1": null}, {"a": [7, 8, 9]});
    y!(nested_19, {"a.1": [8, 9]}, {"a": [7, [8, 9]]});
    n!(nested_20, {"a.1": 6}, {"a": [[6, 7], [8, 9]]});
    n!(nested_21, {"a.1": 7}, {"a": [[6, 7], [8, 9]]});
    n!(nested_22, {"a.1": 8}, {"a": [[6, 7], [8, 9]]});
    n!(nested_23, {"a.1": 9}, {"a": [[6, 7], [8, 9]]});
    y!(nested_24, {"a.1": 2}, {"a": [0, {"1": 2}, 3]});
    y!(nested_25, {"a.1": {"1": 2}}, {"a": [0, {"1": 2}, 3]});
    y!(nested_26, {"x.1.y": 8}, {"x": [7, {"y": 8}, 9]});
    y!(nested_27, {"x.1.y": null}, {"x": [7, {"y": 8}, 9]});
    y!(nested_28, {"a.1.b": 9}, {"a": [7, {"b": 9}, {"1": {"b": "foo"}}]});
    y!(nested_29, {"a.1.b": "foo"}, {"a": [7, {"b": 9}, {"1": {"b": "foo"}}]});
    y!(nested_30, {"a.1.b": null}, {"a": [7, {"b": 9}, {"1": {"b": "foo"}}]});
    y!(nested_31, {"a.1.b": 2}, {"a": [1, [{"b": 2}], 3]});
    n!(nested_32, {"a.1.b": null}, {"a": [1, [{"b": 2}], 3]});
    n!(nested_33, {"a.0.b": null}, {"a": [5]});
    y!(nested_34, {"a.1": 4}, {"a": [{"1": 4}, 5]});
    y!(nested_35, {"a.1": 5}, {"a": [{"1": 4}, 5]});
    n!(nested_36, {"a.1": null}, {"a": [{"1": 4}, 5]});
    y!(nested_37, {"a.1.foo": 4}, {"a": [{"1": {"foo": 4}}, {"foo": 5}]});
    y!(nested_38, {"a.1.foo": 5}, {"a": [{"1": {"foo": 4}}, {"foo": 5}]});
    y!(nested_39, {"a.1.foo": null}, {"a": [{"1": {"foo": 4}}, {"foo": 5}]});
    n!(nested_40, {"a.b": 1}, {"x": 2});
    n!(nested_41, {"a.b.c": 1}, {"a": {"x": 2}});
    n!(nested_42, {"a.b.c": 1}, {"a": {"b": {"x": 2}}});
    n!(nested_43, {"a.b.c": 1}, {"a": {"b": 1}});
    n!(nested_44, {"a.b.c": 1}, {"a": {"b": 0}});
    y!(nested_45, {"a.b": {"c": 1}}, {"a": {"b": {"c": 1}}});
    n!(nested_46, {"a.b": {"c": 1}}, {"a": {"b": {"c": 2}}});
    n!(nested_47, {"a.b": {"c": 1}}, {"a": {"b": 2}});
    y!(nested_48, {"a.b": {"c": 1, "d": 2}}, {"a": {"b": {"c": 1, "d": 2}}});
    n!(nested_49, {"a.b": {"c": 1, "d": 2}}, {"a": {"b": {"c": 1, "d": 1}}});
    n!(nested_50, {"a.b": {"c": 1, "d": 2}}, {"a": {"b": {"d": 2}}});

    // $and.
    y!(operator_and_1, {"$and": [{"a": 1}]}, {"a": 1});
    n!(operator_and_2, {"$and": [{"a": 1}, {"a": 2}]}, {"a": 1});
    n!(operator_and_3, {"$and": [{"a": 1}, {"b": 1}]}, {"a": 1});
    y!(operator_and_4, {"$and": [{"a": 1}, {"b": 2}]}, {"a": 1, "b": 2});
    n!(operator_and_5, {"$and": [{"a": 1}, {"b": 1}]}, {"a": 1, "b": 2});
    y!(operator_and_6, {"$and": [{"a": 1}, {"b": 2}], "c": 3}, {"a": 1, "b": 2, "c": 3});
    n!(operator_and_7, {"$and": [{"a": 1}, {"b": 2}], "c": 4}, {"a": 1, "b": 2, "c": 3});

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
    y!(operator_eq_11, {"a.b": {"$eq": 1}}, {"a": [{"b": 1}, {"b": 2}]});
    y!(operator_eq_12, {"a.b": {"$eq": 2}}, {"a": [{"b": 1}, {"b": 2}]});
    n!(operator_eq_13, {"a.b": {"$eq": 3}}, {"a": [{"b": 1}, {"b": 2}]});

    // $exists.
    y!(operator_exists_01, {"a": {"$exists": true}}, {"a": 12});
    n!(operator_exists_02, {"a": {"$exists": true}}, {"b": 12});
    n!(operator_exists_03, {"a": {"$exists": false}}, {"a": 12});
    y!(operator_exists_04, {"a": {"$exists": false}}, {"b": 12});
    y!(operator_exists_05, {"a": {"$exists": true}}, {"a": []});
    n!(operator_exists_06, {"a": {"$exists": true}}, {"b": []});
    n!(operator_exists_07, {"a": {"$exists": false}}, {"a": []});
    y!(operator_exists_08, {"a": {"$exists": false}}, {"b": []});
    y!(operator_exists_09, {"a": {"$exists": true}}, {"a": [1]});
    n!(operator_exists_10, {"a": {"$exists": true}}, {"b": [1]});
    n!(operator_exists_11, {"a": {"$exists": false}}, {"a": [1]});
    y!(operator_exists_12, {"a": {"$exists": false}}, {"b": [1]});
    n!(operator_exists_13, {"a.x": {"$exists": false}}, {"a": [{}, {"x": 5}]});
    y!(operator_exists_14, {"a.x": {"$exists": true}}, {"a": [{}, {"x": 5}]});
    y!(operator_exists_15, {"a.x": {"$exists": true}}, {"a": [{}, {"x": 5}]});
    y!(operator_exists_16, {"a.x": {"$exists": true}}, {"a": {"x": []}});
    y!(operator_exists_17, {"a.x": {"$exists": true}}, {"a": {"x": null}});

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
    y!(operator_in_13, {"a.b": {"$in": [1, null]}}, {});
    y!(operator_in_14, {"a.b": {"$in": [1, null]}}, {"a": {}});
    y!(operator_in_15, {"a.b": {"$in": [1, null]}}, {"a": {"b": null}});
    n!(operator_in_16, {"a.b": {"$in": [1, null]}}, {"a": {"b": 5}});
    n!(operator_in_17, {"a.b": {"$in": [1]}}, {"a": {"b": null}});
    n!(operator_in_18, {"a.b": {"$in": [1]}}, {"a": {}});
    n!(operator_in_19, {"a.b": {"$in": [1, null]}}, {"a": [{"b": 5}]});
    y!(operator_in_20, {"a.b": {"$in": [1, null]}}, {"a": [{"b": 5}, {}]});
    n!(operator_in_21, {"a.b": {"$in": [1, null]}}, {"a": [{"b": 5}, []]});
    n!(operator_in_22, {"a.b": {"$in": [1, null]}}, {"a": [{"b": 5}, 5]});
    y!(operator_in_23, {"a.b": {"$in": [1, 2, 3]}}, {"a": {"b": [2]}});
    y!(operator_in_24, {"a.b": {"$in": [{"x": 1}, {"x": 2}, {"x": 3}]}}, {"a": {"b": [{"x": 2}]}});
    y!(operator_in_25, {"a.b": {"$in": [1, 2, 3]}}, {"a": {"b": [4, 2]}});
    n!(operator_in_26, {"a.b": {"$in": [1, 2, 3]}}, {"a": {"b": [4]}});

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
    n!(operator_ne_11, {"a.b": {"$ne": 1}}, {"a": [{"b": 1}, {"b": 2}]});
    n!(operator_ne_12, {"a.b": {"$ne": 2}}, {"a": [{"b": 1}, {"b": 2}]});
    y!(operator_ne_13, {"a.b": {"$ne": 3}}, {"a": [{"b": 1}, {"b": 2}]});

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
    n!(operator_nin_13, {"a.b": {"$nin": [1, 2, 3]}}, {"a": [{"b": 4}, {"b": 2}]});
    y!(operator_nin_14, {"a.b": {"$nin": [1, 2, 3]}}, {"a": [{"b": 4}]});
    n!(operator_nin_15, {"a.b": {"$nin": [1, null]}}, {});
    n!(operator_nin_16, {"a.b": {"$nin": [1, null]}}, {"a": {}});
    n!(operator_nin_17, {"a.b": {"$nin": [1, null]}}, {"a": {"b": null}});
    y!(operator_nin_18, {"a.b": {"$nin": [1, null]}}, {"a": {"b": 5}});
    y!(operator_nin_19, {"a.b": {"$nin": [1]}}, {"a": {"b": null}});
    y!(operator_nin_20, {"a.b": {"$nin": [1]}}, {"a": {}});
    y!(operator_nin_21, {"a.b": {"$nin": [1, null]}}, {"a": [{"b": 5}]});
    n!(operator_nin_22, {"a.b": {"$nin": [1, null]}}, {"a": [{"b": 5}, {}]});
    y!(operator_nin_23, {"a.b": {"$nin": [1, null]}}, {"a": [{"b": 5}, []]});
    y!(operator_nin_24, {"a.b": {"$nin": [1, null]}}, {"a": [{"b": 5}, 5]});

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

    // $size.
    y!(operator_size_01, {"a": {"$size": 0}}, {"a": []});
    y!(operator_size_02, {"a": {"$size": 1}}, {"a": [2]});
    y!(operator_size_03, {"a": {"$size": 2}}, {"a": [2, 2]});
    n!(operator_size_04, {"a": {"$size": 0}}, {"a": [2]});
    n!(operator_size_05, {"a": {"$size": 1}}, {"a": []});
    n!(operator_size_06, {"a": {"$size": 1}}, {"a": [2, 2]});
    n!(operator_size_07, {"a": {"$size": 0}}, {"a": "2"});
    n!(operator_size_08, {"a": {"$size": 1}}, {"a": "2"});
    n!(operator_size_09, {"a": {"$size": 2}}, {"a": "2"});
    n!(operator_size_10, {"a": {"$size": 2}}, {"a": [[2, 2]]});

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
    n!(operators_gt_lt_1, {"a": {"$lt": 11, "$gt": 9}}, {"a": 8});
    n!(operators_gt_lt_2, {"a": {"$lt": 11, "$gt": 9}}, {"a": 9});
    y!(operators_gt_lt_3, {"a": {"$lt": 11, "$gt": 9}}, {"a": 10});
    n!(operators_gt_lt_4, {"a": {"$lt": 11, "$gt": 9}}, {"a": 11});
    n!(operators_gt_lt_5, {"a": {"$lt": 11, "$gt": 9}}, {"a": 12});
    y!(operators_gt_lt_6, {"a": {"$lt": 11, "$gt": 9}}, {"a": [8, 9, 10, 11, 12]});
    y!(operators_gt_lt_7, {"a": {"$lt": 11, "$gt": 9}}, {"a": [8, 9, 11, 12]});

    // $gt + $ne.
    y!(operators_gt_ne_1, {"a": {"$ne": 5, "$gt": 6}}, {"a": [2, 10]});
    n!(operators_gt_ne_2, {"a": {"$ne": 5, "$gt": 6}}, {"a": [2, 4]});
    n!(operators_gt_ne_3, {"a": {"$ne": 5, "$gt": 6}}, {"a": [2, 5]});
    n!(operators_gt_ne_4, {"a": {"$ne": 5, "$gt": 6}}, {"a": [10, 5]});
    y!(operators_gt_ne_5, {"a.b": {"$ne": 5, "$gt": 6}}, {"a": [{"b": 2}, {"b": 10}]});
    n!(operators_gt_ne_6, {"a.b": {"$ne": 5, "$gt": 6}}, {"a": [{"b": 2}, {"b": 4}]});
    n!(operators_gt_ne_7, {"a.b": {"$ne": 5, "$gt": 6}}, {"a": [{"b": 2}, {"b": 5}]});
    n!(operators_gt_ne_8, {"a.b": {"$ne": 5, "$gt": 6}}, {"a": [{"b": 10}, {"b": 5}]});

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
