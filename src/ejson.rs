use base64::engine::{general_purpose::STANDARD, Engine};
use bson::oid::ObjectId;
use bson::{Bson, DateTime, Decimal128, Document};
use serde_json::{json, Map, Number, Value};
use std::str::FromStr;

pub fn from_ejson(bson: &mut Bson) {
    let Bson::Document(document) = bson else {
        return;
    };

    let mut keys: Vec<_> = document.keys().map(String::as_str).collect();
    keys.sort_unstable();
    match keys.as_slice() {
        ["$date"] => {
            if let Ok(date) = document.get_i64("$date") {
                *bson = Bson::DateTime(DateTime::from_millis(date));
            }
        }
        ["$type"] => match document.get_str("$type") {
            Ok("Decimal") => {
                if let Ok(value) = document.get_str("$value") {
                    if let Ok(value) = Decimal128::from_str(value) {
                        *bson = Bson::Decimal128(value);
                    }
                }
            }
            Ok("oid") => {
                if let Ok(value) = document.get_str("$value") {
                    if let Ok(value) = ObjectId::from_str(value) {
                        *bson = Bson::ObjectId(value);
                    }
                }
            }
            _ => {}
        },
        _ => {}
    }
}

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
