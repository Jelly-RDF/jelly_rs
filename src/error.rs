use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeserializeError {
    #[error("Missing term in term triple")]
    MissingTermInTermTriple,
    #[error("lookup error {0}")]
    LookupError(#[from] LookupError),
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
