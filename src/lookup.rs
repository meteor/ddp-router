use serde_json::Value;

#[derive(Clone, Debug)]
pub struct Branch<'a> {
    pub dont_iterate: bool,
    pub value: Option<&'a Value>,
}

impl Branch<'_> {
    pub fn expand(branches: Vec<Self>, skip_the_arrays: bool) -> Vec<Self> {
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
pub struct Lookup {
    for_sort: bool,
    key: String,
    key_as_usize: Option<usize>,
    rest: Option<Box<Lookup>>,
}

impl Lookup {
    pub fn lookup<'a>(&'a self, value: &'a Value) -> Vec<Branch> {
        let Self {
            for_sort,
            key,
            key_as_usize,
            rest,
        } = &self;

        if let Value::Array(values) = value {
            if !key_as_usize.is_some_and(|index| index < values.len()) {
                return vec![];
            }
        }

        let value_head = match value {
            Value::Array(values) => key_as_usize.and_then(|index| values.get(index)),
            Value::Object(values) => values.get(key),
            _ => None,
        };

        let Some(rest) = rest else {
            return vec![Branch {
                value: value_head,
                dont_iterate: value.is_array() && value_head.is_some_and(Value::is_array),
            }];
        };

        let Some(value_head) = value_head
            .filter(|value_head| matches!(value_head, Value::Array(_) | Value::Object(_)))
        else {
            return if value.is_array() {
                vec![]
            } else {
                vec![Branch {
                    value: None,
                    dont_iterate: false,
                }]
            };
        };

        let mut result = rest.lookup(value_head);
        if rest.key_as_usize.is_none() || !*for_sort {
            if let Value::Array(branches) = value_head {
                for branch in branches {
                    // TODO: Exclude BSON-specific types.
                    if branch.is_object() {
                        result.extend(rest.lookup(branch));
                    }
                }
            }
        }

        result
    }

    pub fn new(key: String, for_sort: bool) -> Self {
        let (key, rest) = match key.split_once('.') {
            Some((key, rest)) => (
                key.to_owned(),
                Some(Box::new(Self::new(rest.to_owned(), for_sort))),
            ),
            None => (key, None),
        };
        let key_as_usize = key.parse::<usize>().ok();

        Self {
            for_sort,
            key,
            key_as_usize,
            rest,
        }
    }
}
