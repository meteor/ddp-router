use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Meteor {
    pub url: String,
}

#[derive(Deserialize)]
pub struct Mongo {
    pub url: String,
}

#[derive(Deserialize)]
pub struct Router {
    pub url: String,
}

#[derive(Deserialize)]
pub struct Settings {
    pub meteor: Meteor,
    pub mongo: Mongo,
    pub router: Router,
}

impl Settings {
    pub fn from(path: &str) -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name(path).required(false))
            .add_source(Environment::default().ignore_empty(true).separator("_"))
            .build()?
            .try_deserialize()
    }
}
