use serde_json::Value;
use std::collections::BTreeMap;

pub struct Inflight {
    pub name: String,
    pub params: Option<Vec<Value>>,
    update_received: bool,
}

impl Inflight {
    pub fn new(name: String, params: Option<Vec<Value>>) -> Self {
        Self {
            name,
            params,
            update_received: false,
        }
    }
}

#[derive(Default)]
pub struct Inflights(BTreeMap<String, Option<Inflight>>);

impl Inflights {
    pub fn process_result(&mut self, id: &String) -> Option<Inflight> {
        let update_received = self
            .0
            .get(id)?
            .as_ref()
            .is_some_and(|inflight| inflight.update_received);
        if update_received {
            self.0.remove(id)?
        } else {
            self.0.get_mut(id)?.take()
        }
    }

    pub fn process_update(&mut self, id: &String) -> bool {
        if let Some(maybe_inflight) = self.0.get_mut(id) {
            if let Some(inflight) = maybe_inflight {
                inflight.update_received = true;
            } else {
                self.0.remove(id);
            }
            true
        } else {
            false
        }
    }

    pub fn register(&mut self, id: String, inflight: Inflight) {
        self.0.insert(id, Some(inflight));
    }
}
