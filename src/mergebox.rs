use crate::ddp::DDPMessage;
use crate::ejson::IntoEjson;
use anyhow::{anyhow, Error};
use bson::Document;
use serde_json::Value;
use std::collections::BTreeMap;
use std::mem::take;

#[derive(Default)]
pub struct Mergebox {
    collections: BTreeMap<String, Vec<MergeboxDocument>>,
    pub messages: Vec<DDPMessage>,
}

impl Mergebox {
    pub fn flush(&mut self) -> Vec<DDPMessage> {
        take(&mut self.messages)
    }

    pub fn insert(&mut self, collection: &str, id: Value, document: Document) -> Result<(), Error> {
        let mergebox_collection = self.collections.entry(collection.to_owned()).or_default();
        let maybe_mergebox_index = mergebox_collection.iter().position(|x| x.id == id);
        if let Some(mergebox_index) = maybe_mergebox_index {
            let fields = mergebox_collection[mergebox_index].change(document);
            if !fields.is_empty() {
                self.messages.push(DDPMessage::Changed {
                    collection: collection.to_owned(),
                    id,
                    fields: Some(fields.into_ejson()),
                    cleared: None,
                });
            }
        } else {
            mergebox_collection.push(MergeboxDocument::new(id.clone(), document.clone()));
            self.messages.push(DDPMessage::Added {
                collection: collection.to_owned(),
                id,
                fields: if document.is_empty() {
                    None
                } else {
                    Some(document.into_ejson())
                },
            });
        }

        Ok(())
    }

    pub fn remove(
        &mut self,
        collection: &str,
        id: Value,
        document: &Document,
    ) -> Result<(), Error> {
        let mergebox_collection = self
            .collections
            .get_mut(collection)
            .ok_or_else(|| anyhow!("Collection {collection} not found"))?;
        let mergebox_index = mergebox_collection
            .iter()
            .position(|x| x.id == id)
            .ok_or_else(|| anyhow!("Document {id} not found"))?;
        let cleared = mergebox_collection[mergebox_index].remove(document)?;
        if mergebox_collection[mergebox_index].count == 0 {
            mergebox_collection.swap_remove(mergebox_index);
            self.messages.push(DDPMessage::Removed {
                collection: collection.to_owned(),
                id,
            });
        } else if !cleared.is_empty() {
            self.messages.push(DDPMessage::Changed {
                collection: collection.to_owned(),
                id,
                fields: None,
                cleared: Some(cleared),
            });
        }
        Ok(())
    }
}

pub struct MergeboxDocument {
    id: Value,
    count: usize,
    fields: BTreeMap<String, MergeboxField>,
}

impl MergeboxDocument {
    pub fn change(&mut self, document: Document) -> Document {
        self.count += 1;

        document
            .into_iter()
            .filter(|(field, value)| {
                let value_as_ejson = value.clone().into_ejson();
                if let Some(mergebox_field) = self.fields.get_mut(field) {
                    let is_changed = mergebox_field.value != value_as_ejson;
                    mergebox_field.count += 1;
                    mergebox_field.value = value_as_ejson;
                    is_changed
                } else {
                    self.fields
                        .insert(field.clone(), MergeboxField::new(value_as_ejson));
                    true
                }
            })
            .collect()
    }

    pub fn new(id: Value, document: Document) -> Self {
        let fields = document
            .into_iter()
            .map(|(field, value)| (field, MergeboxField::new(value.into_ejson())))
            .collect();
        Self {
            id,
            count: 1,
            fields,
        }
    }

    pub fn remove(&mut self, document: &Document) -> Result<Vec<String>, Error> {
        self.count -= 1;

        let mut cleared = Vec::default();
        for field in document.keys() {
            let count = &mut self
                .fields
                .get_mut(field)
                .ok_or_else(|| anyhow!("Field {field} not found"))?
                .count;
            if *count == 1 {
                self.fields.remove(field);
                cleared.push(field.clone());
            } else {
                *count -= 1;
            }
        }

        Ok(cleared)
    }
}

pub struct MergeboxField {
    count: usize,
    value: Value,
}

impl MergeboxField {
    pub fn new(value: Value) -> Self {
        Self { count: 1, value }
    }
}
