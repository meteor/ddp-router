use crate::ejson::into_ejson;
use crate::lookup::{Branch, Lookup};
use crate::sorter::Sorter;
use anyhow::{anyhow, Error};
use bson::{Bson, Document, Regex as BsonRegex};
use regex::{Regex, RegexBuilder};
use serde_json::{Map, Value};
use std::cmp::Ordering;

#[derive(Debug)]
pub enum DocumentMatcher {
    All(Vec<Self>),
    Any(Vec<Self>),
    Invert(Box<Self>),
    Lookup {
        #[allow(private_interfaces)]
        lookup: Lookup,
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
                .filter(|(key, _)| key.as_str() != "$comment") // Ignore it.
                .map(|(key, sub_selector)| {
                    if key.starts_with('$') {
                        Self::compile_logical_operator(key, sub_selector, is_in_elem_match)
                    } else {
                        Ok(Self::Lookup {
                            lookup: Lookup::new(key.to_owned(), false),
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
            "$nor" => Self::compile_many(selector, is_in_elem_match)
                .map(Self::any)
                .map(Self::invert),
            operator => Err(anyhow!("{operator} is not supported")),
        }
    }

    fn compile_many(selector: &Bson, is_in_elem_match: bool) -> Result<Vec<Self>, Error> {
        let selectors = selector
            .as_array()
            .ok_or_else(|| anyhow!("Expected array of selectors, got {selector:?}"))?;
        if selectors.is_empty() {
            return Err(anyhow!("Expected non-empty array of selectors"));
        }

        selectors
            .iter()
            .map(|selector| {
                let document = selector
                    .as_document()
                    .ok_or_else(|| anyhow!("Expected document selector, got {selector:?}"))?;
                Self::compile_inner(document, is_in_elem_match, false)
            })
            .collect()
    }

    fn invert(self) -> Self {
        Self::Invert(Box::new(self))
    }

    pub fn matches(&self, document: &Map<String, Value>) -> bool {
        match &self {
            Self::All(matchers) => matchers.iter().all(|matcher| matcher.matches(document)),
            Self::Any(matchers) => matchers.iter().any(|matcher| matcher.matches(document)),
            Self::Invert(matcher) => !matcher.matches(document),
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
    Never,
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
        selector: &Document,
        _is_root: bool,
    ) -> Result<Self, Error> {
        match operator {
            "$all" => {
                let operands = operand
                    .as_array()
                    .ok_or_else(|| anyhow!("$all expected an array, got {operand:?}"))?;
                if operands.is_empty() {
                    return Ok(Self::Never);
                }

                Ok(Self::all(
                    operands
                        .iter()
                        .map(|operand| {
                            if is_operator_object(operand).is_some() {
                                Err(anyhow!("$all expected plain document, got {operand:?}"))
                            } else {
                                Ok(ElementMatcher::compile(operand)?.into_branched(false, false))
                            }
                        })
                        .collect::<Result<_, _>>()?,
                ))
            }
            "$eq" => Ok(ElementMatcher::compile(operand)?.into_branched(false, false)),
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
                        if is_operator_object(operand).is_some() {
                            Err(anyhow!("$in expected plain document, got {operand:?}"))
                        } else {
                            Ok(ElementMatcher::compile(operand)?.into_branched(false, false))
                        }
                    })
                    .collect::<Result<_, _>>()?,
            )),
            "$mod" => {
                let operands = operand
                    .as_array()
                    .ok_or_else(|| anyhow!("$mod expected an array, got {operand:?}"))?;
                if operands.len() != 2 {
                    return Err(anyhow!("$mod expected 2 arguments, got {operand:?}"));
                }

                let parse = |value: &Bson| -> Result<i64, Error> {
                    Ok(match value {
                        Bson::Double(n) if n.is_finite() => n.trunc() as i64,
                        Bson::Int32(n) => *n as i64,
                        Bson::Int64(n) => *n,
                        value => return Err(anyhow!("$mod expected a number, got {value:?}")),
                    })
                };

                let div = parse(&operands[0])?;
                let rem = parse(&operands[1])?;

                Ok(ElementMatcher::Mod(div, rem).into_branched(false, false))
            }
            "$ne" => Self::compile_operator("$eq", operand, selector, _is_root).map(Self::invert),
            "$nin" => Self::compile_operator("$in", operand, selector, _is_root).map(Self::invert),
            "$not" => Self::compile_value_selector(operand, false).map(Self::invert),
            "$regex" => {
                let (pattern, options) = match operand {
                    Bson::RegularExpression(regex) => {
                        (regex.pattern.to_owned(), regex.options.as_str())
                    }
                    Bson::String(pattern) => (pattern.to_owned(), ""),
                    operand => {
                        return Err(anyhow!(
                            "$regex expected a regex or string, got {operand:?}"
                        ))
                    }
                };

                let options = selector
                    .get("$options")
                    .map_or_else(
                        || Ok(options),
                        |options| {
                            options.as_str().ok_or_else(|| {
                                anyhow!("$options expected a string, got {options:?}")
                            })
                        },
                    )?
                    .to_owned();

                let regex = BsonRegex { pattern, options };
                Self::compile_value_selector(&Bson::RegularExpression(regex), false)
            }
            // TODO: This could be optimized out.
            "$options" if selector.contains_key("$regex") => Ok(Self::Never.invert()),
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
            "$type" => {
                let type_ = match operand {
                    Bson::Int32(operand) => match operand {
                        1..=5 | 7..=11 => *operand as i8,
                        operand => return Err(anyhow!("$type got an unknown number: {operand}")),
                    },
                    Bson::String(operand) => match operand.as_str() {
                        "double" => 1,
                        "string" => 2,
                        "object" => 3,
                        "array" => 4,
                        "binData" => 5,
                        "objectId" => 7,
                        "bool" => 8,
                        "date" => 9,
                        "null" => 10,
                        "regex" => 11,
                        operand => return Err(anyhow!("$type got an unknown string: {operand}")),
                    },
                    operand => {
                        return Err(anyhow!(
                            "$type expected a number or string, got {operand:?}"
                        ))
                    }
                };

                Ok(ElementMatcher::Type(type_).into_branched(false, true))
            }
            operator => Err(anyhow!("{operator} is not supported")),
        }
    }

    fn compile_value_selector(selector: &Bson, is_root: bool) -> Result<Self, Error> {
        if let Some(selector) = is_operator_object(selector) {
            Ok(Self::all(
                selector
                    .iter()
                    .map(|(operator, operand)| {
                        Self::compile_operator(operator, operand, selector, is_root)
                    })
                    .collect::<Result<_, _>>()?,
            ))
        } else {
            Ok(ElementMatcher::compile(selector)?.into_branched(false, false))
        }
    }

    fn invert(self) -> Self {
        match self {
            Self::Invert(inverted) => *inverted,
            not_inverted => Self::Invert(Box::new(not_inverted)),
        }
    }

    fn matches(&self, branches: Vec<Branch>) -> bool {
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
                    expanded = Branch::expand(expanded, *dont_include_leaf_arrays);
                }

                expanded
                    .into_iter()
                    .any(|element| matcher.matches(element.value))
            }
            Self::Invert(matcher) => !matcher.matches(branches),
            Self::Never => false,
        }
    }
}

#[derive(Debug)]
enum ElementMatcher {
    Exists,
    Mod(i64, i64),
    Order {
        selector: Value,
        ordering: Ordering,
        is_negated: bool,
    },
    Regex(Regex, Value),
    Size(usize),
    Type(i8),
    Value(Value),
}

impl ElementMatcher {
    fn compile(selector: &Bson) -> Result<Self, Error> {
        match selector {
            Bson::Array(_)
            | Bson::Binary(_)
            | Bson::Boolean(_)
            | Bson::DateTime(_)
            | Bson::Decimal128(_)
            | Bson::Document(_)
            | Bson::Double(_)
            | Bson::Int32(_)
            | Bson::Int64(_)
            | Bson::Null
            | Bson::ObjectId(_)
            | Bson::String(_) => Ok(Self::Value(into_ejson(selector.clone()))),
            Bson::DbPointer(_)
            | Bson::JavaScriptCode(_)
            | Bson::JavaScriptCodeWithScope(_)
            | Bson::MaxKey
            | Bson::MinKey
            | Bson::Symbol(_)
            | Bson::Timestamp(_)
            | Bson::Undefined => Err(anyhow!("Selector not supported: {selector:?}")),
            Bson::RegularExpression(regex) => {
                let mut regex_builder = RegexBuilder::new(&regex.pattern);
                for flag in regex.options.chars() {
                    match flag {
                        'i' => regex_builder.case_insensitive(true),
                        'm' => regex_builder.multi_line(true),
                        's' => regex_builder.dot_matches_new_line(true),
                        'x' => regex_builder.ignore_whitespace(true),
                        flag => return Err(anyhow!("Unknown $regex flag {flag}")),
                    };
                }

                let regex = regex_builder.build()?;
                let ejson = into_ejson(selector.clone());
                Ok(Self::Regex(regex, ejson))
            }
        }
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
            // TODO: Check how MongoDB handles $mod of floats.
            Self::Mod(div, rem) => maybe_value
                .and_then(Value::as_i64)
                .is_some_and(|number| number % div == *rem),
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
                let result = Sorter::cmp_value_partial(value, selector);
                result.is_ok_and(|result| result == *ordering) != *is_negated
            }
            Self::Regex(regex, ejson) => maybe_value.is_some_and(|value| match value {
                Value::Object(_) => value == ejson,
                Value::String(string) => regex.is_match(string),
                _ => false,
            }),
            Self::Size(size) => maybe_value
                .and_then(Value::as_array)
                .is_some_and(|array| array.len() == *size),
            Self::Type(type_) => maybe_value
                .map(Sorter::value_type)
                .is_some_and(|value_type| value_type == *type_),
            Self::Value(Value::Null) => maybe_value.map_or(true, Value::is_null),
            Self::Value(selector) => {
                maybe_value.is_some_and(|value| Sorter::cmp_value(selector, value).is_eq())
            }
        }
    }
}

fn is_operator_object(selector: &Bson) -> Option<&Document> {
    selector.as_document().filter(|selector| {
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
    use bson::oid::ObjectId;
    use bson::{doc, Binary, DateTime, Regex};

    macro_rules! regex {
        ($pattern:expr) => {
            regex!($pattern, "")
        };
        ($pattern:expr, $options:expr) => {
            Regex {
                pattern: $pattern.to_owned(),
                options: $options.to_owned(),
            }
        };
    }

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

    macro_rules! f {
        ($name:ident, { $($selector:tt)* }) => {
            #[test]
            fn $name() {
                let selector = &doc! { $($selector)* };
                if let Ok(matcher) = DocumentMatcher::compile(selector) {
                    panic!("{selector:?} should not be supported, got {matcher:?}");
                }
            }
        };
    }

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

    // Object.
    y!(object_01, {"a": {"b": 12}}, {"a": {"b": 12}});
    n!(object_02, {"a": {"b": 12, "c": 13}}, {"a": {"b": 12}});
    n!(object_03, {"a": {"b": 12}}, {"a": {"b": 12, "c": 13}});
    y!(object_04, {"a": {"b": 12, "c": 13}}, {"a": {"b": 12, "c": 13}});
    n!(object_05, {"a": {"b": 12, "c": 13}}, {"a": {"c": 13, "b": 12}});
    n!(object_06, {"a": {}}, {"a": {"b": 12}});
    n!(object_07, {"a": {"b": 12}}, {"a": {}});
    y!(object_08, {"a": {"b": 12, "c": [13, true, false, 2.2, "a", null, {"d": 14}]}}, {"a": {"b": 12, "c": [13, true, false, 2.2, "a", null, {"d": 14}]}});
    y!(object_09, {"a": {"b": 12}}, {"a": {"b": 12}, "k": 99});
    y!(object_10, {"a": {"b": 12}}, {"a": [{"b": 12}]});
    n!(object_11, {"a": {"b": 12}}, {"a": [[{"b": 12}]]});
    y!(object_12, {"a": {"b": 12}}, {"a": [{"b": 11}, {"b": 12}, {"b": 13}]});
    n!(object_13, {"a": {"b": 12}}, {"a": [{"b": 11}, {"b": 12, "c": 20}, {"b": 13}]});
    n!(object_14, {"a": {"b": 12, "c": 20}}, {"a": [{"b": 11}, {"b": 12}, {"c": 20}]});
    y!(object_15, {"a": {"b": 12, "c": 20}}, {"a": [{"b": 11}, {"b": 12, "c": 20}, {"b": 13}]});

    // Regex.
    y!(regex_01, {"a": regex!("a") }, {"a": "cat"});
    n!(regex_02, {"a": regex!("a") }, {"a": "cut"});
    n!(regex_03, {"a": regex!("a") }, {"a": "CAT"});
    y!(regex_04, {"a": regex!("a", "i") }, {"a": "CAT"});
    y!(regex_05, {"a": regex!("a") }, {"a": ["foo", "bar"]});
    n!(regex_06, {"a": regex!(",") }, {"a": ["foo", "bar"]});
    y!(regex_07, {"a": {"$regex": "a"}}, {"a": ["foo", "bar"]});
    n!(regex_08, {"a": {"$regex": ","}}, {"a": ["foo", "bar"]});
    y!(regex_09, {"a": {"$regex": regex!("a") }}, {"a": "cat"});
    n!(regex_10, {"a": {"$regex": regex!("a") }}, {"a": "cut"});
    n!(regex_11, {"a": {"$regex": regex!("a") }}, {"a": "CAT"});
    y!(regex_12, {"a": {"$regex": regex!("a", "i") }}, {"a": "CAT"});
    y!(regex_13, {"a": {"$regex": regex!("a"), "$options": "i"}}, {"a": "CAT"});
    y!(regex_14, {"a": {"$regex": regex!("a", "i"), "$options": "i"}}, {"a": "CAT"});
    n!(regex_15, {"a": {"$regex": regex!("a", "i"), "$options": ""}}, {"a": "CAT"});
    y!(regex_16, {"a": {"$regex": "a"}}, {"a": "cat"});
    n!(regex_17, {"a": {"$regex": "a"}}, {"a": "cut"});
    n!(regex_18, {"a": {"$regex": "a"}}, {"a": "CAT"});
    y!(regex_19, {"a": {"$regex": "a", "$options": "i"}}, {"a": "CAT"});
    y!(regex_20, {"a": {"$regex": "", "$options": "i"}}, {"a": "foo"});
    n!(regex_21, {"a": {"$regex": "", "$options": "i"}}, {});
    n!(regex_22, {"a": {"$regex": "", "$options": "i"}}, {"a": 5});
    n!(regex_23, {"a": regex!("undefined") }, {});
    n!(regex_24, {"a": {"$regex": "undefined"}}, {});
    n!(regex_25, {"a": regex!("xxx") }, {});
    n!(regex_26, {"a": {"$regex": "xxx"}}, {});
    y!(regex_27, {"a": regex!("a")}, {"a": ["dog", "cat"]});
    n!(regex_28, {"a": regex!("a")}, {"a": ["dog", "puppy"]});
    y!(regex_29, {"a": regex!("a")}, {"a": regex!("a")});
    y!(regex_30, {"a": regex!("a")}, {"a": ["x", regex!("a")]});
    n!(regex_31, {"a": regex!("a")}, {"a": regex!("a", "i")});
    n!(regex_32, {"a": regex!("a", "m")}, {"a": regex!("a")});
    n!(regex_33, {"a": regex!("a")}, {"a": regex!("b")});
    n!(regex_34, {"a": regex!("5")}, {"a": 5});
    n!(regex_35, {"a": regex!("t")}, {"a": true});
    y!(regex_36, {"a": regex!("m", "i")}, {"a": ["x", "xM"]});

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
    y!(nested_51, {"a.b": regex!("a")}, {"a": {"b": "cat"}});
    n!(nested_52, {"a.b": regex!("a")}, {"a": {"b": "dog"}});

    // $all.
    y!(operator_all_01, {"a": {"$all": [1, 2]}}, {"a": [1, 2]});
    n!(operator_all_02, {"a": {"$all": [1, 2, 3]}}, {"a": [1, 2]});
    y!(operator_all_03, {"a": {"$all": [1, 2]}}, {"a": [3, 2, 1]});
    y!(operator_all_04, {"a": {"$all": [1, "x"]}}, {"a": [3, "x", 1]});
    n!(operator_all_05, {"a": {"$all": ["2"]}}, {"a": 2});
    n!(operator_all_06, {"a": {"$all": [2]}}, {"a": "2"});
    y!(operator_all_07, {"a": {"$all": [[1, 2], [1, 3]]}}, {"a": [[1, 3], [1, 2], [1, 4]]});
    n!(operator_all_08, {"a": {"$all": [[1, 2], [1, 3]]}}, {"a": [[1, 4], [1, 2], [1, 4]]});
    y!(operator_all_09, {"a": {"$all": [2, 2]}}, {"a": [2]});
    n!(operator_all_10, {"a": {"$all": [2, 3]}}, {"a": [2, 2]});
    n!(operator_all_11, {"a": {"$all": [1, 2]}}, {"a": [[1, 2]]});
    n!(operator_all_12, {"a": {"$all": [1, 2]}}, {});
    n!(operator_all_13, {"a": {"$all": [1, 2]}}, {"a": {"foo": "bar"}});
    n!(operator_all_14, {"a": {"$all": []}}, {"a": []});
    n!(operator_all_15, {"a": {"$all": []}}, {"a": [5]});
    y!(operator_all_16, {"a": {"$all": [{"b": 3}]}}, {"a": [{"b": 3}]});
    n!(operator_all_17, {"a": {"$all": [{"b": 3}]}}, {"a": [{"b": 3, "k": 4}]});
    f!(operator_all_18, {"a": {"$all": [{"$gt": 4}]}});

    // $and.
    y!(operator_and_01, {"$and": [{"a": 1}]}, {"a": 1});
    n!(operator_and_02, {"$and": [{"a": 1}, {"a": 2}]}, {"a": 1});
    n!(operator_and_03, {"$and": [{"a": 1}, {"b": 1}]}, {"a": 1});
    y!(operator_and_04, {"$and": [{"a": 1}, {"b": 2}]}, {"a": 1, "b": 2});
    n!(operator_and_05, {"$and": [{"a": 1}, {"b": 1}]}, {"a": 1, "b": 2});
    y!(operator_and_06, {"$and": [{"a": 1}, {"b": 2}], "c": 3}, {"a": 1, "b": 2, "c": 3});
    n!(operator_and_07, {"$and": [{"a": 1}, {"b": 2}], "c": 4}, {"a": 1, "b": 2, "c": 3});
    f!(operator_and_08, {"$and": []});
    f!(operator_and_09, {"$and": [5]});
    y!(operator_and_10, {"$and": [{"a": regex!("a")}]}, {"a": "cat"});
    y!(operator_and_11, {"$and": [{"a": regex!("a", "i")}]}, {"a": "CAT"});
    n!(operator_and_12, {"$and": [{"a": regex!("o")}]}, {"a": "cat"});
    n!(operator_and_13, {"$and": [{"a": regex!("a")}, {"a": regex!("o")}]}, {"a": "cat"});
    y!(operator_and_14, {"$and": [{"a": regex!("a")}, {"b": regex!("o")}]}, {"a": "cat", "b": "dog"});
    n!(operator_and_15, {"$and": [{"a": regex!("a")}, {"b": regex!("a")}]}, {"a": "cat", "b": "dog"});

    // $comment.
    y!(operator_comment_1, {"a": 5, "$comment": "Some text..."}, {"a": 5});
    n!(operator_comment_2, {"a": 6, "$comment": "Some text..."}, {"a": 5});

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

    // $mod.
    y!(operator_mod_01, {"a": {"$mod": [10, 1]}}, {"a": 11});
    n!(operator_mod_02, {"a": {"$mod": [10, 1]}}, {"a": 12});
    y!(operator_mod_03, {"a": {"$mod": [10, 1]}}, {"a": [10, 11, 12]});
    n!(operator_mod_04, {"a": {"$mod": [10, 1]}}, {"a": [10, 12]});
    f!(operator_mod_05, {"a": {"$mod": 5}});
    f!(operator_mod_06, {"a": {"$mod": [10]}});
    f!(operator_mod_07, {"a": {"$mod": [10, 1, 2]}});
    f!(operator_mod_08, {"a": {"$mod": "foo"}});
    f!(operator_mod_09, {"a": {"$mod": {"bar": 1}}});
    f!(operator_mod_10, {"a": {"$mod": []}});

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

    // $nor.
    n!(operator_nor_1, {"$nor": [{"a": regex!("a")}]}, {"a": "cat"});
    y!(operator_nor_2, {"$nor": [{"a": regex!("o")}]}, {"a": "cat"});
    n!(operator_nor_3, {"$nor": [{"a": regex!("a")}, {"a": regex!("o")}]}, {"a": "cat"});
    y!(operator_nor_4, {"$nor": [{"a": regex!("i")}, {"a": regex!("o")}]}, {"a": "cat"});
    n!(operator_nor_5, {"$nor": [{"a": regex!("i")}, {"b": regex!("o")}]}, {"a": "cat", "b": "dog"});

    // $not.
    y!(operator_not_01, {"x": {"$not": {"$gt": 7}}}, {"x": 6});
    n!(operator_not_02, {"x": {"$not": {"$gt": 7}}}, {"x": 8});
    y!(operator_not_03, {"x": {"$not": {"$lt": 10, "$gt": 7}}}, {"x": 11});
    n!(operator_not_04, {"x": {"$not": {"$lt": 10, "$gt": 7}}}, {"x": 9});
    y!(operator_not_05, {"x": {"$not": {"$lt": 10, "$gt": 7}}}, {"x": 6});
    y!(operator_not_06, {"x": {"$not": {"$gt": 7}}}, {"x": [2, 3, 4]});
    y!(operator_not_07, {"x.y": {"$not": {"$gt": 7}}}, {"x": [{"y": 2}, {"y": 3}, {"y": 4}]});
    n!(operator_not_08, {"x": {"$not": {"$gt": 7}}}, {"x": [2, 3, 4, 10]});
    n!(operator_not_09, {"x.y": {"$not": {"$gt": 7}}}, {"x": [{"y": 2}, {"y": 3}, {"y": 4}, {"y": 10}]});
    y!(operator_not_10, {"x": {"$not": regex!("a")}}, {"x": "dog"});
    n!(operator_not_11, {"x": {"$not": regex!("a")}}, {"x": "cat"});
    y!(operator_not_12, {"x": {"$not": regex!("a")}}, {"x": ["dog", "puppy"]});
    n!(operator_not_13, {"x": {"$not": regex!("a")}}, {"x": ["kitten", "cat"]});

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
    f!(operator_or_18, {"$or": []});
    f!(operator_or_19, {"$or": [5]});
    y!(operator_or_20, {"$or": [{"a": regex!("a")}]}, {"a": "cat"});
    n!(operator_or_21, {"$or": [{"a": regex!("o")}]}, {"a": "cat"});
    y!(operator_or_22, {"$or": [{"a": regex!("a")}, {"a": regex!("o")}]}, {"a": "cat"});
    n!(operator_or_23, {"$or": [{"a": regex!("i")}, {"a": regex!("o")}]}, {"a": "cat"});
    y!(operator_or_24, {"$or": [{"a": regex!("i")}, {"b": regex!("o")}]}, {"a": "cat", "b": "dog"});

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

    // $type.
    y!(operator_type_1, {"a": {"$type": 1}}, {"a": 1.1});
    y!(operator_type_2, {"a": {"$type": "double"}}, {"a": 1.1});
    y!(operator_type_3, {"a": {"$type": 1}}, {"a": 1});
    n!(operator_type_4, {"a": {"$type": 1}}, {"a": "1"});
    y!(operator_type_5, {"a": {"$type": 2}}, {"a": "1"});
    y!(operator_type_6, {"a": {"$type": "string"}}, {"a": "1"});
    n!(operator_type_7, {"a": {"$type": 2}}, {"a": 1});
    y!(operator_type_8, {"a": {"$type": 3}}, {"a": {}});
    y!(operator_type_9, {"a": {"$type": "object"}}, {"a": {}});
    y!(operator_type_10, {"a": {"$type": 3}}, {"a": {"b": 2}});
    n!(operator_type_11, {"a": {"$type": 3}}, {"a": []});
    n!(operator_type_12, {"a": {"$type": 3}}, {"a": [1]});
    n!(operator_type_13, {"a": {"$type": 3}}, {"a": null});
    y!(operator_type_14, {"a": {"$type": 5}}, {"a": Binary::from_base64("", None).unwrap()});
    y!(operator_type_15, {"a": {"$type": "binData"}}, {"a": Binary::from_base64("", None).unwrap()});
    y!(operator_type_16, {"a": {"$type": 5}}, {"a": Binary::from_base64("", None).unwrap()});
    n!(operator_type_17, {"a": {"$type": 5}}, {"a": []});
    n!(operator_type_18, {"a": {"$type": 5}}, {"a": [42]});
    y!(operator_type_19, {"a": {"$type": 7}}, {"a": ObjectId::new()});
    y!(operator_type_20, {"a": {"$type": "objectId"}}, {"a": ObjectId::new()});
    n!(operator_type_21, {"a": {"$type": 7}}, {"a": "1234567890abcd1234567890"});
    y!(operator_type_22, {"a": {"$type": 8}}, {"a": true});
    y!(operator_type_23, {"a": {"$type": "bool"}}, {"a": true});
    y!(operator_type_24, {"a": {"$type": 8}}, {"a": false});
    n!(operator_type_25, {"a": {"$type": 8}}, {"a": "true"});
    n!(operator_type_26, {"a": {"$type": 8}}, {"a": 0});
    n!(operator_type_27, {"a": {"$type": 8}}, {"a": null});
    n!(operator_type_28, {"a": {"$type": 8}}, {"a": ""});
    n!(operator_type_29, {"a": {"$type": 8}}, {});
    y!(operator_type_30, {"a": {"$type": 9}}, {"a": DateTime::from_millis(0)});
    y!(operator_type_31, {"a": {"$type": "date"}}, {"a": DateTime::from_millis(0)});
    n!(operator_type_32, {"a": {"$type": 9}}, {"a": 0});
    y!(operator_type_33, {"a": {"$type": 10}}, {"a": null});
    y!(operator_type_34, {"a": {"$type": "null"}}, {"a": null});
    n!(operator_type_35, {"a": {"$type": 10}}, {"a": false});
    n!(operator_type_36, {"a": {"$type": 10}}, {"a": ""});
    n!(operator_type_37, {"a": {"$type": 10}}, {"a": 0});
    n!(operator_type_38, {"a": {"$type": 10}}, {});
    y!(operator_type_39, {"a": {"$type": 11}}, {"a": regex!("x") });
    y!(operator_type_40, {"a": {"$type": "regex"}}, {"a": regex!("x") });
    n!(operator_type_41, {"a": {"$type": 11}}, {"a": "x"});
    n!(operator_type_42, {"a": {"$type": 11}}, {});
    n!(operator_type_43, {"a": {"$type": 4}}, {"a": []});
    n!(operator_type_44, {"a": {"$type": 4}}, {"a": [1]});
    y!(operator_type_45, {"a": {"$type": 1}}, {"a": [1]});
    n!(operator_type_46, {"a": {"$type": 2}}, {"a": [1]});
    y!(operator_type_47, {"a": {"$type": 1}}, {"a": ["1", 1]});
    y!(operator_type_48, {"a": {"$type": 2}}, {"a": ["1", 1]});
    n!(operator_type_49, {"a": {"$type": 3}}, {"a": ["1", 1]});
    n!(operator_type_50, {"a": {"$type": 4}}, {"a": ["1", 1]});
    n!(operator_type_51, {"a": {"$type": 1}}, {"a": ["1", []]});
    y!(operator_type_52, {"a": {"$type": 2}}, {"a": ["1", []]});
    y!(operator_type_53, {"a": {"$type": 4}}, {"a": ["1", []]});
    y!(operator_type_54, {"a.0": {"$type": 4}}, {"a": [[0]]});
    y!(operator_type_55, {"a.0": {"$type": "array"}}, {"a": [[0]]});
    n!(operator_type_56, {"a.0": {"$type": 1}}, {"a": [[0]]});
    f!(operator_type_57, {"a": {"$type": "foo"}});
    f!(operator_type_58, {"a": {"$type": -2}});
    f!(operator_type_59, {"a": {"$type": 0}});
    f!(operator_type_60, {"a": {"$type": 20}});

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

    // $and + $not.
    y!(operators_and_not_1, {"$and": [{"a": {"$not": {"$gt": 2}}}]}, {"a": 1});
    n!(operators_and_not_2, {"$and": [{"a": {"$not": {"$lt": 2}}}]}, {"a": 1});
    y!(operators_and_not_3, {"$and": [{"a": {"$not": {"$lt": 0}}}, {"a": {"$not": {"$gt": 2}}}]}, {"a": 1});
    n!(operators_and_not_4, {"$and": [{"a": {"$not": {"$lt": 2}}}, {"a": {"$not": {"$gt": 0}}}]}, {"a": 1});

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

    // $nor + $not.
    n!(operators_nor_not_1, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}]}, {});
    y!(operators_nor_not_2, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}]}, {"a": 1});
    n!(operators_nor_not_3, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}]}, {"a": 2});
    n!(operators_nor_not_4, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$not": {"$mod": [10, 2]}}}]}, {"a": 1});
    y!(operators_nor_not_5, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$mod": [10, 2]}}]}, {"a": 1});
    n!(operators_nor_not_6, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$mod": [10, 2]}}]}, {"a": 2});
    n!(operators_nor_not_7, {"$nor": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$mod": [10, 2]}}]}, {"a": 3});

    // $not + $or.
    y!(operators_not_or_1, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}]}, {});
    n!(operators_not_or_2, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}]}, {"a": 1});
    y!(operators_not_or_3, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}]}, {"a": 2});
    y!(operators_not_or_4, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$not": {"$mod": [10, 2]}}}]}, {"a": 1});
    n!(operators_not_or_5, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$mod": [10, 2]}}]}, {"a": 1});
    y!(operators_not_or_6, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$mod": [10, 2]}}]}, {"a": 2});
    y!(operators_not_or_7, {"$or": [{"a": {"$not": {"$mod": [10, 1]}}}, {"a": {"$mod": [10, 2]}}]}, {"a": 3});
}
