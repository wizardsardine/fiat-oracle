//! Library surface for integration tests in `tests/`.
//!
//! `main.rs` keeps its own `mod` declarations for the binary build; this
//! file re-exposes the same modules as a library so cross-binary
//! integration tests (`tests/depth_bounds.rs`, etc.) can `use fiat_oracle::…`.

// `HistoricalSource::make_pager` is part of the public trait but takes a
// `pub(crate)` type (`BoxedPager`). That's an intentional design — pagers
// are an internal implementation detail. The lint flags it once we expose
// `HistoricalSource` via this lib crate; ignore.
#![allow(private_interfaces)]

pub mod api;
pub mod cache;
pub mod config;
pub mod currency;
pub mod error;
pub mod estimator;
pub mod fetcher;
pub mod paging;
pub mod state;
pub mod storage;
