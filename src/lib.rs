pub mod cli;
pub mod innodb;
pub mod util;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum IdbError {
    #[error("I/O error: {0}")]
    Io(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Invalid argument: {0}")]
    Argument(String),
}
