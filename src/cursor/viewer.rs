use super::description::CursorDescription;
use crate::matcher::DocumentMatcher;
use crate::projector::Projector;
use crate::sorter::Sorter;
use anyhow::{ensure, Context, Error};

#[derive(Debug)]
pub struct CursorViewer {
    pub matcher: DocumentMatcher,
    pub projector: Projector,
    pub sorter: Sorter,
}

impl TryFrom<&CursorDescription> for CursorViewer {
    type Error = Error;
    fn try_from(description: &CursorDescription) -> Result<Self, Self::Error> {
        let CursorDescription {
            disable_oplog,
            limit,
            projection,
            selector,
            skip,
            sort,
            ..
        } = description;

        let matcher = DocumentMatcher::compile(selector)
            .with_context(|| format!("selector {selector:?} is not supported"))?;
        let projector = Projector::compile(projection.as_ref())
            .with_context(|| format!("projection {projection:?} is not supported"))?;
        let sorter = Sorter::compile(sort.as_ref())
            .with_context(|| format!("sort {sort:?} is not supported"))?;

        ensure!(limit.is_none() || sort.is_some(), "limit requires sort");
        ensure!(skip.is_none(), "skip is not supported");
        ensure!(!*disable_oplog, "explicitly disabled");

        Ok(Self {
            matcher,
            projector,
            sorter,
        })
    }
}
