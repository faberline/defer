//! The `defer` integration tests, linked into one test binary `it`
//! (faberline/aw docs/standards/rust-tests.md). Each module is one former
//! `tests/<name>.rs` binary; `e2e` holds the cluster suites. `operator.rs`
//! stays its own binary because CI runs it by name with `--features operator`.

mod cli_contract;
mod cli_efficiency;
mod cli_stability;
mod competitor_feature_matrix;
mod direct_k8s_assets;
mod e2e;
mod http_api;
mod http_dispatch;
mod http_dispatch_signing;
mod process_journey;
mod raft_peer_mtls;
mod raft_scheduler;
mod rate_limits;
mod relay_performance_ceiling;
mod service_admission;
mod service_auth;
mod task_lifecycle;
