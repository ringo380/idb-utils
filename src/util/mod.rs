//! Shared utilities (hex dump formatting, filesystem helpers, optional MySQL connectivity).

pub mod fs;
pub mod hex;
#[cfg(feature = "mysql")]
pub mod mysql;
