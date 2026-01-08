use std::sync::Once;

static INIT_RUSTLS: Once = Once::new();

pub fn init_rustls_crypto_provider() {
    INIT_RUSTLS.call_once(|| {
        let provider = rustls::crypto::ring::default_provider();
        let _ = provider.install_default();
    });
}

