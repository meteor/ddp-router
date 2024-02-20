use config::{Config, ConfigError};
use serde::{Deserialize};

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Database {
    pub url: String,
}
#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Router {
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Meteor {
   pub url: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub database: Database,
    pub router: Router,
    pub meteor: Meteor,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(config::File::with_name("./config/Default"))
            .add_source(config::File::with_name("./config/Local").required(false))
            .build()?;

        s.try_deserialize()
    }
}
