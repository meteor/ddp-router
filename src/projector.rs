use anyhow::{anyhow, Error};
use bson::{Bson, Document};
use serde_json::{Map, Value};

pub enum Projector {
    Empty,
    Exclude(Vec<String>),
    Include(Vec<String>),
}

impl Projector {
    pub fn apply(&self, mut document: Map<String, Value>) -> Map<String, Value> {
        match self {
            Self::Empty => document,
            Self::Exclude(paths) => {
                document.retain(|key, _| paths.binary_search(key).is_err());
                document
            }
            Self::Include(paths) => {
                document.retain(|key, _| paths.binary_search(key).is_ok());
                document
            }
        }
    }

    pub fn compile(projection: Option<&Document>) -> Result<Self, Error> {
        let Some(projection) = projection else {
            return Ok(Self::Empty);
        };

        let mut paths = vec![];
        let mut include_all = None;
        let mut include_id = None;

        for (path, operator) in projection {
            let include = match operator {
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

            if path.contains('.') {
                return Err(anyhow!("Nested projections are not supported"));
            }

            match include_all {
                Some(include_all) => {
                    if include_all != include {
                        return Err(anyhow!("Projection cannot be both exclusive and inclusive"));
                    }
                }
                None => include_all = Some(include),
            }

            paths.push(path.clone());
        }

        match (include_all, include_id) {
            (Some(true), None) => include_id = Some(true),
            (None, include_id) => include_all = include_id,
            _ => {}
        }

        if include_all == include_id {
            paths.push("_id".to_owned());
        }

        paths.sort_unstable();

        Ok(match include_all {
            Some(true) => Self::Include(paths),
            Some(false) => Self::Exclude(paths),
            None => Self::Empty,
        })
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
                let Value::Object(input) = input else { unreachable!() };
                let Value::Object(output) = output else { unreachable!() };

                let projector = match Projector::compile(projection) {
                    Ok(projector) => projector,
                    Err(error) => panic!("{projection:?} is not supported: {error:?}"),
                };

                assert_eq!(projector.apply(input), output);
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
}
