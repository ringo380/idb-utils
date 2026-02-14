//! Shared utilities (hex dump formatting, filesystem helpers, optional MySQL connectivity).

#[cfg(feature = "cli")]
pub mod fs;
pub mod hex;
#[cfg(feature = "mysql")]
pub mod mysql;
