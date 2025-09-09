use prost::UnknownEnumValue;
use thiserror::Error;

use crate::proto::PhysicalStreamType;

#[derive(Error, Debug)]
pub enum DeserializeError {
    #[error("{0}")]
    ConfigError(#[from] ConfigError),
    #[error("{0}")]
    PhysicalStreamError(#[from] PhysicalStreamError),
    #[error("Missing term triple term in {0:?}")]
    MissingTermTermTriple(TermLocation),
    #[error("Missing term in {0:?}")]
    MissingTerm(TermLocation),
    #[error("lookup error {0}")]
    LookupError(#[from] LookupError),

    #[error("{0} is not implemented")]
    NotImplemented(&'static str),

    // Sophia errors
    #[cfg(feature = "sophia")]
    #[error("Missing term in term triple {0}")]
    InvalidLanguageTag(#[from] sophia_api::term::language_tag::InvalidLanguageTag),
    #[cfg(feature = "sophia")]
    #[error("Missing term in term triple {0}")]
    InvalidIri(#[from] sophia_iri::InvalidIri),
}

#[derive(Debug)]
pub enum TermLocation {
    Subject,
    Predicate,
    Object,
    Graph,
}

#[derive(Error, Debug)]
pub enum PhysicalStreamError {
    #[error("invalid physical stream type {detected:?} for message {incoming:?}")]
    IncorrectType {
        detected: PhysicalStreamType,
        incoming: MessageType,
    },
    #[error("{expected:?} is not yet set but is required for {detected:?}")]
    NotYetSet {
        detected: PhysicalStreamType,
        expected: MessageType,
    },
    #[error("Unspecified physical stream type is not supported")]
    UnspecifiedStreamType,
}

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Physical Stream Type {0:?} is not supported")]
    InvalidPhysicalType(PhysicalStreamType),
    #[error("Physical Stream Type not specified ({0:?})")]
    InvalidPhysicalNotSet(#[from] UnknownEnumValue),
    #[error("Table {table:?} to large ({set} > {max})")]
    TableToLarge { table: Table, set: u32, max: u32 },
    #[error("No config set")]
    NotSet,
}
impl ConfigError {
    pub fn name_table(set: u32) -> Option<Self> {
        let max = 4096;
        (set > max).then_some(ConfigError::TableToLarge {
            table: Table::NameTable,
            set,
            max,
        })
    }

    pub fn prefix_table(set: u32) -> Option<Self> {
        let max = 1024;
        (set > max).then_some(ConfigError::TableToLarge {
            table: Table::PrefixTable,
            set,
            max,
        })
    }

    pub fn datatype_table(set: u32) -> Option<Self> {
        let max = 256;
        (set > max).then_some(ConfigError::TableToLarge {
            table: Table::DatatypeTable,
            set,
            max,
        })
    }
}

#[derive(Debug)]
pub enum Table {
    NameTable,
    DatatypeTable,
    PrefixTable,
}

#[derive(Debug)]
pub enum MessageType {
    Quad,
    Triple,
    GraphStart,
    GraphEnd,
}

#[derive(Error, Debug)]
pub enum LookupError {
    #[error("Trying to lookup from empty table")]
    LookupFromEmptyTable,
    #[error("Lookup index {0} {1} is out of bounds")]
    Missing(usize, usize),
    #[error("Lookup table with size {0} overflowing")]
    LookupTableTooSmall(usize),
    #[error("Invalid lookup type, this should not happen with 0")]
    InvalidLookupAction,
}
