// HANDWRITE-BEGIN gap="missing-generator:logic:defer-service-auth" tracker="#766" reason="Defer adapter for the shared bearer registry and per-queue role authorization."
//! Defer's bearer-token access: the configured verifier and per-queue role
//! authorization.

mod policy;

pub use policy::{
    authorize, AuthConfig, AUTH_MODE_ENV, LEGACY_TOKENS_ENV, TOKEN_REGISTRY_FILE_ENV,
};
// HANDWRITE-END
