use base64::engine::{general_purpose::STANDARD, Engine};
use bson::{Bson, Document};
use serde_json::{json, Map, Number, Value};

pub fn into_ejson(bson: Bson) -> Value {
    match bson {
        // Meteor EJSON serialization.
        Bson::Binary(v) => json!({ "$binary": STANDARD.encode(v.bytes)}),
        Bson::DateTime(v) => json!({ "$date": v.timestamp_millis() }),
        Bson::Decimal128(v) => json!({ "$type": "Decimal", "$value": v.to_string() }),
        Bson::Double(v) if v.is_infinite() => json!({ "$InfNaN": v.signum() }),
        Bson::Double(v) if v.is_nan() => json!({ "$InfNan": 0 }),
        Bson::ObjectId(v) => json!({ "$type": "oid", "$value": v.to_hex() }),
        Bson::RegularExpression(v) => json!({ "$regexp": v.pattern, "$flags": v.options }),

        // Standard JSON serialization.
        Bson::Array(v) => Value::Array(v.into_iter().map(into_ejson).collect()),
        Bson::Boolean(v) => Value::Bool(v),
        Bson::Document(v) => Value::Object(into_ejson_document(v)),
        Bson::Double(v) => Value::Number(Number::from_f64(v).unwrap()),
        Bson::Int32(v) => Value::Number(Number::from(v)),
        Bson::Int64(v) => Value::Number(Number::from(v)),
        Bson::Null => Value::Null,
        Bson::String(v) => Value::String(v),

        // Replace everything else with `null`s.
        v => {
            println!("Unrecognized BSON value found: {v}");
            Value::Null
        }
    }
}

pub fn into_ejson_document(document: Document) -> Map<String, Value> {
    document
        .into_iter()
        .map(|(k, v)| (k, into_ejson(v)))
        .collect()
}
