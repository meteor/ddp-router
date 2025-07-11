use crate::ddp::DDPMessage;
use anyhow::{anyhow, Context, Error};
use serde_json::{Map, Value};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

type Document = Map<String, Value>;

#[derive(Default)]
pub struct Mergeboxes(BTreeMap<usize, (usize, Arc<Mutex<Mergebox>>)>);

impl Mergeboxes {
    pub async fn insert(
        &mut self,
        collection: String,
        id: Value,
        document: Document,
    ) -> Result<(), Error> {
        for (_, mergebox) in self.0.values_mut() {
            mergebox
                .lock()
                .await
                .insert(collection.clone(), id.clone(), document.clone())
                .await
                .context("Mergeboxes::insert")?;
        }

        Ok(())
    }

    pub fn insert_mergebox(&mut self, session_id: usize, mergebox: &Arc<Mutex<Mergebox>>) -> bool {
        let is_first = self.0.is_empty();
        self.0
            .entry(session_id)
            .and_modify(|(counter, _)| *counter += 1)
            .or_insert_with(|| (0, mergebox.clone()));
        is_first
    }

    pub async fn remove(
        &mut self,
        collection: String,
        id: Value,
        document: &Document,
    ) -> Result<(), Error> {
        for (_, mergebox) in self.0.values_mut() {
            mergebox
                .lock()
                .await
                .remove(collection.clone(), id.clone(), document)
                .await
                .context("Mergeboxes::remove")?;
        }

        Ok(())
    }

    pub fn remove_mergebox(&mut self, session_id: usize) -> bool {
        if let Entry::Occupied(mut entry) = self.0.entry(session_id) {
            if entry.get().0 == 0 {
                entry.remove();
                return self.0.is_empty();
            }

            entry.get_mut().0 -= 1;
        }

        false
    }
}

pub struct Mergebox {
    collections: BTreeMap<String, Vec<MergeboxDocument>>,
    server_view: BTreeMap<String, Vec<(Value, Document)>>,
    messages_sink: Sender<DDPMessage>,
}

impl Mergebox {
    pub async fn insert(
        &mut self,
        collection: String,
        id: Value,
        document: Document,
    ) -> Result<(), Error> {
        let mergebox_collection = self.collections.entry(collection.clone()).or_default();
        let maybe_mergebox_index = mergebox_collection.iter().position(|x| x.id == id);
        if let Some(mergebox_index) = maybe_mergebox_index {
            let fields = mergebox_collection[mergebox_index].change(document);
            if !fields.is_empty() {
                self.messages_sink
                    .send(DDPMessage::Changed {
                        collection,
                        id,
                        fields: Some(fields),
                        cleared: None,
                    })
                    .await?;
            }
        } else {
            mergebox_collection.push(MergeboxDocument::new(id.clone(), document.clone()));
            self.messages_sink
                .send(DDPMessage::Added {
                    collection,
                    id,
                    fields: if document.is_empty() {
                        None
                    } else {
                        Some(document)
                    },
                    cleared: None,
                })
                .await?;
        }

        Ok(())
    }

    pub fn new(messages_sink: Sender<DDPMessage>) -> Self {
        Self {
            collections: BTreeMap::default(),
            server_view: BTreeMap::default(),
            messages_sink,
        }
    }

    pub async fn remove(
        &mut self,
        collection: String,
        id: Value,
        document: &Document,
    ) -> Result<(), Error> {
        let mergebox_collection = self
            .collections
            .get_mut(&collection)
            .ok_or_else(|| anyhow!("Collection {collection} not found"))?;
        let mergebox_index = mergebox_collection
            .iter()
            .position(|x| x.id == id)
            .ok_or_else(|| anyhow!("Document {id} not found in {collection}"))?;
        let cleared = mergebox_collection[mergebox_index]
            .remove(document)
            .with_context(|| format!("Remove {id} from {collection}"))?;

        if mergebox_collection[mergebox_index].count == 0 {
            mergebox_collection.swap_remove(mergebox_index);
            self.messages_sink
                .send(DDPMessage::Removed { collection, id })
                .await?;
        } else if !cleared.is_empty() {
            self.messages_sink
                .send(DDPMessage::Changed {
                    collection,
                    id,
                    fields: None,
                    cleared: Some(cleared),
                })
                .await?;
        }

        Ok(())
    }

    pub async fn server_added(
        &mut self,
        collection: String,
        id: Value,
        fields: Option<Document>,
    ) -> Result<(), Error> {
        // Update `server_view`.
        let document = fields.unwrap_or_default();
        self.server_view
            .entry(collection.clone())
            .or_default()
            .push((id.clone(), document.clone()));

        // Update `collections`.
        self.insert(collection, id, document).await
    }

    pub async fn server_changed(
        &mut self,
        collection: String,
        id: Value,
        fields: Option<Document>,
        cleared: Option<Vec<String>>,
    ) -> Result<(), Error> {
        // Update `server_view`.
        let documents = self
            .server_view
            .get_mut(&collection)
            .ok_or_else(|| anyhow!("Collection not found {collection}"))?;
        let index = documents
            .iter()
            .position(|x| x.0 == id)
            .ok_or_else(|| anyhow!("Document not found {id} in {collection}"))?;
        let document = documents.swap_remove(index).1;
        let mut document_applied = document.clone();
        for field in cleared.into_iter().flatten() {
            document_applied.remove(&field);
        }
        for (key, value) in fields.into_iter().flatten() {
            document_applied.insert(key, value);
        }
        documents.push((id.clone(), document_applied.clone()));

        // Update `collections`.
        self.insert(collection.clone(), id.clone(), document_applied)
            .await
            .context("Mergebox::server_changed")?;
        self.remove(collection, id, &document)
            .await
            .context("Mergebox::server_changed")
    }

    pub async fn server_removed(&mut self, collection: String, id: Value) -> Result<(), Error> {
        // Update `server_view`.
        let documents = self
            .server_view
            .get_mut(&collection)
            .ok_or_else(|| anyhow!("Collection not found {collection}"))?;
        let index = documents
            .iter()
            .position(|x| x.0 == id)
            .ok_or_else(|| anyhow!("Document not found {id} in {collection}"))?;
        let document = documents.swap_remove(index).1;

        // Update `collections`.
        self.remove(collection, id, &document)
            .await
            .context("Mergebox::server_removed")
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
                if let Some(mergebox_field) = self.fields.get_mut(field) {
                    mergebox_field.count += 1;
                    if mergebox_field.value != *value {
                        mergebox_field.value = value.clone();
                        true
                    } else {
                        false
                    }
                } else {
                    self.fields
                        .insert(field.clone(), MergeboxField::new(value.clone()));
                    true
                }
            })
            .collect()
    }

    pub fn new(id: Value, document: Document) -> Self {
        let fields = document
            .into_iter()
            .map(|(field, value)| (field, MergeboxField::new(value)))
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
