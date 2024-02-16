use anyhow::{anyhow, Error};
use bson::{Bson, Document};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub struct Projector(Tree, bool);

impl Projector {
    pub fn apply(&self, document: &mut Map<String, Value>) {
        self.0.apply_document(document, self.1);
    }

    pub fn compile(projection: Option<&Document>) -> Result<Self, Error> {
        let mut tree = Tree::default();
        let mut include_all = None;
        let mut include_id = None;

        for (path, operator) in projection.into_iter().flatten() {
            let include = match operator {
                Bson::Boolean(boolean) => *boolean,
                Bson::Int32(1) => true,
                Bson::Int32(0) => false,
                operator => {
                    return Err(anyhow!("Projection {operator} for {path} is not supported"))
                }
            };

            // _id is special.
            if path == "_id" {
                include_id = Some(include);
                continue;
            }

            match include_all {
                Some(include_all) => {
                    if include_all != include {
                        return Err(anyhow!("Projection cannot be both exclusive and inclusive"));
                    }
                }
                None => include_all = Some(include),
            }

            tree.add(path.as_str());
        }

        match (include_all, include_id) {
            (Some(true), None) => include_id = Some(true),
            (None, include_id) => include_all = include_id,
            _ => {}
        }

        if include_all == include_id {
            tree.add("_id");
        }

        Ok(Self(tree, include_all.unwrap_or(false)))
    }
}

#[derive(Default)]
enum Tree {
    #[default]
    Leaf,
    Node(BTreeMap<String, Tree>),
}

impl Tree {
    fn add(&mut self, key: &str) {
        let map = match self {
            Self::Leaf => {
                *self = Self::Node(BTreeMap::new());
                return self.add(key);
            }
            Self::Node(map) => map,
        };

        if let Some((key, path)) = key.split_once('.') {
            map.entry(key.to_owned()).or_default().add(path);
        } else {
            map.entry(key.to_owned()).or_default();
        }
    }

    fn apply_document(&self, document: &mut Map<String, Value>, include: bool) {
        if let Self::Node(map) = self {
            document.retain(|key, value| match map.get(key) {
                Some(Self::Leaf) => include,
                Some(tree) => {
                    tree.apply_value(value, include);
                    true
                }
                None => !include,
            });
        }
    }

    fn apply_value(&self, value: &mut Value, include: bool) {
        match value {
            Value::Array(values) => values
                .iter_mut()
                .for_each(|value| self.apply_value(value, include)),
            Value::Object(value) => self.apply_document(value, include),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Projector;
    use bson::doc;
    use serde_json::{json, Value};

    macro_rules! test {
        ($name:ident, { $($projection:tt)* }, { $($input:tt)* }, { $($output:tt)* }) => {
            #[test]
            fn $name() {
                let projection = doc! { $($projection)* };
                let input = json! {{ $($input)* }};
                let output = json! {{ $($output)* }};

                let projection = Some(&projection);
                let Value::Object(mut input) = input else { unreachable!() };
                let Value::Object(output) = output else { unreachable!() };

                let projector = match Projector::compile(projection) {
                    Ok(projector) => projector,
                    Err(error) => panic!("{projection:?} is not supported: {error:?}"),
                };

                projector.apply(&mut input);
                assert_eq!(input, output);
            }
        };
    }

    test!(empty, {}, {"a": 7, "b": 8}, {"a": 7, "b": 8});

    test!(exclude_1, {"a": 0}, {}, {});
    test!(exclude_2, {"a": 0}, {"a": 7}, {});
    test!(exclude_3, {"a": 0}, {"a": 7, "b": 8}, {"b": 8});
    test!(exclude_4, {"a": 0}, {"a": 7, "b": 8, "_id": 9}, {"b": 8, "_id": 9});
    test!(exclude_5, {"a": 0, "_id": 0}, {"a": 7, "b": 8, "_id": 9}, {"b": 8});
    test!(exclude_6, {"a": 0, "_id": 1}, {"a": 7, "b": 8, "_id": 9}, {"b": 8, "_id": 9});

    test!(include_1, {"a": 1}, {}, {});
    test!(include_2, {"a": 1}, {"a": 7}, {"a": 7});
    test!(include_3, {"a": 1}, {"a": 7, "b": 8}, {"a": 7});
    test!(include_4, {"a": 1}, {"a": 7, "b": 8, "_id": 9}, {"a": 7, "_id": 9});
    test!(include_5, {"a": 1, "_id": 0}, {"a": 7, "b": 8, "_id": 9}, {"a": 7});
    test!(include_6, {"a": 1, "_id": 1}, {"a": 7, "b": 8, "_id": 9}, {"a": 7, "_id": 9});

    test!(nested_01, {"a.b.c": 1}, {}, {});
    test!(nested_02, {"a.b.c": 1}, {"a": []}, {"a": []});
    test!(nested_03, {"a.b.c": 1}, {"a": {}}, {"a": {}});
    test!(nested_04, {"a.b.c": 1}, {"a": {"b": []}}, {"a": {"b": []}});
    test!(nested_05, {"a.b.c": 1}, {"a": {"b": {}}}, {"a": {"b": {}}});
    test!(nested_06, {"a.b.c": 1}, {"a": {"b": {"c": 7}}}, {"a": {"b": {"c": 7}}});
    test!(nested_07, {"a.b.c": 1}, {"a": {"b": {"c": 7}, "c": 8}, "d": 9}, {"a": {"b": {"c": 7}}});
    test!(nested_08, {"a.b.c": 1}, {"a": {"b": {"c": [7]}}}, {"a": {"b": {"c": [7]}}});
    test!(nested_09, {"a.b.c": 1}, {"a": {"b": [{"c": 7}]}}, {"a": {"b": [{"c": 7}]}});
    test!(nested_10, {"a.b.c": 1}, {"a": [{"b": {"c": 7}}]}, {"a": [{"b": {"c": 7}}]});
    test!(nested_11, {"a.b.c": 0}, {"a": {"b": {"c": 7}, "c": 8}, "d": 9}, {"a": {"b": {}, "c": 8}, "d": 9});
}
