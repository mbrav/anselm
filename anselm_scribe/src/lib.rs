pub mod config;
pub mod db;
pub mod metrics;
pub mod models;
pub mod runners;

// A simple type alias so as to DRY.
// type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
