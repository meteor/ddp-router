use bson::{Bson, Document};
use serde_json::{Map, Value};

pub fn apply(projection: Option<&Document>, document: Map<String, Value>) -> Map<String, Value> {
    let Some(projection) = projection else {
        return document;
    };

    document
        .into_iter()
        .filter(|(field, _)| projection.contains_key(field) || field == "_id")
        .collect()
}

pub fn is_supported(projection: Option<&Document>) -> bool {
    let Some(projection) = projection else {
        return true;
    };

    projection.iter().all(|(key, projection)| {
        // TODO: Implement nested keys.
        if key.contains('.') {
            return false;
        }

        // TODO: Implement operators.
        // TODO: Implement field exclusions.
        *projection == Bson::Int32(1)
    })
}

#[cfg(test)]
mod tests {
    use super::{apply, is_supported};
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

                assert!(is_supported(projection), "{projection:?} is not supported");
                assert_eq!(apply(projection, input), output);
            }
        };
    }

    test!(include_1, {"a": 1}, {}, {});
    test!(include_2, {"a": 1}, {"a": 7}, {"a": 7});
    test!(include_3, {"a": 1}, {"a": 7, "b": 9}, {"a": 7});
    test!(include_4, {"a": 1}, {"a": 7, "_id": 9}, {"a": 7, "_id": 9});
}
