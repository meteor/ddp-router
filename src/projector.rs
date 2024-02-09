use bson::{Bson, Document};
use serde_json::{Map, Value};

pub fn apply(projection: Option<&Document>, document: Map<String, Value>) -> Map<String, Value> {
    let Some(projection) = projection else {
        return document;
    };

    document
        .into_iter()
        .filter(|(field, _)| projection.contains_key(field))
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
