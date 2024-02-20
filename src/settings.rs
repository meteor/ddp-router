use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Meteor {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct Mongo {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct Router {
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub meteor: Meteor,
    pub mongo: Mongo,
    pub router: Router,
}

impl Settings {
    pub fn from(path: &str) -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(File::with_name(path))
            .add_source(Environment::default().separator("_"))
            .build()?
            .try_deserialize()
    }
}
