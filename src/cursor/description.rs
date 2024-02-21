use bson::Document;
use mongodb::options::FindOptions;
use serde::{Deserialize, Deserializer};

#[derive(Clone, Debug, PartialEq)]
pub struct CursorDescription {
    pub collection: String,
    pub disable_oplog: bool,
    pub limit: Option<i64>,
    pub polling_interval_ms: Option<u64>,
    pub projection: Option<Document>,
    pub selector: Document,
    pub skip: Option<u64>,
    pub sort: Option<Document>,
    pub transform: Option<()>,
}

impl CursorDescription {
    pub fn as_find_options(&self) -> FindOptions {
        FindOptions::builder()
            .limit(self.limit)
            .projection(self.projection.clone())
            .skip(self.skip)
            .sort(self.sort.clone())
            .build()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit.map(|limit| limit.unsigned_abs() as usize)
    }
}

impl<'de> Deserialize<'de> for CursorDescription {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Description {
            #[serde(rename = "collectionName")]
            collection: String,
            selector: Document,
            options: Options,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Options {
            #[serde(default, rename = "disableOplog")]
            disable_oplog: bool,
            limit: Option<i64>,
            #[serde(rename = "pollingIntervalMs")]
            polling_interval_ms: Option<u64>,
            projection: Option<Document>,
            skip: Option<u64>,
            sort: Option<Document>,
            transform: Option<()>,
        }

        let Description {
            collection,
            selector,
            options:
                Options {
                    disable_oplog,
                    limit,
                    polling_interval_ms,
                    projection,
                    skip,
                    sort,
                    transform,
                },
        } = Description::deserialize(deserializer)?;

        Ok(Self {
            collection,
            disable_oplog,
            limit,
            polling_interval_ms,
            projection,
            selector,
            skip,
            sort,
            transform,
        })
    }
}
