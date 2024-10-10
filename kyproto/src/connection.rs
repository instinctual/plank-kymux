#[cfg(all(feature = "kynet-webtransport-js", target_family = "wasm"))]
pub use kynet::webtransport_js;

use crate::auth::UnauthenticatedConnection;
use crate::{Connection, ConnectionError};
use async_trait::async_trait;

#[allow(dead_code)]
#[async_trait]
pub trait Server: Send + Sync {
    async fn accept(&self) -> Result<Connection, ConnectionError>;

    async fn accept_with_auth(&self) -> Result<UnauthenticatedConnection, ConnectionError>;

    fn close(&self, error_code: u32, reason: &str);

    async fn wait_idle(&self);
}

#[cfg(all(feature = "kynet-quinn", not(target_family = "wasm")))]
pub mod quinn {
    use crate::auth::UnauthenticatedConnection;
    use crate::Connection;

    use async_trait::async_trait;
    use std::net::SocketAddr;

    use kynet::cert::{Certificate, PrivateKey};
    use kynet::error::ConnectionError;
    use kynet::Server;

    pub use kynet::quinn::{QuinnClientOptions, QuinnServerOptions};

    // Wrapper returning a Connection on accept()
    pub struct QuinnServer(kynet::quinn::QuinnServer);

    impl QuinnServer {
        pub fn start_on_addr(
            addr: SocketAddr,
            cert_chain: Vec<Certificate>,
            key: PrivateKey,
            options: &QuinnServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server = kynet::quinn::QuinnServer::start_on_addr(addr, cert_chain, key, options)?;
            Ok(Self(server))
        }

        pub fn start(
            port: u16,
            cert_chain: Vec<Certificate>,
            key: PrivateKey,
            options: &QuinnServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server = kynet::quinn::QuinnServer::start(port, cert_chain, key, options)?;
            Ok(Self(server))
        }
    }

    #[async_trait]
    impl super::Server for QuinnServer {
        async fn accept(&self) -> Result<Connection, ConnectionError> {
            let conn = self.0.accept().await?;
            let kyproto = Connection::accept(conn).await?;
            Ok(kyproto)
        }

        async fn accept_with_auth(&self) -> Result<UnauthenticatedConnection, ConnectionError> {
            let conn = self.0.accept().await?;
            let unauth_kyproto = Connection::accept_with_auth(conn).await?;
            Ok(unauth_kyproto)
        }

        fn close(&self, error_code: u32, reason: &str) {
            self.0.close(error_code, reason);
        }

        async fn wait_idle(&self) {
            self.0.wait_idle().await;
        }
    }
}

#[cfg(all(feature = "kynet-wtransport", not(target_family = "wasm")))]
pub mod wtransport {
    use crate::auth::UnauthenticatedConnection;
    use crate::Connection;

    use async_trait::async_trait;
    use std::net::SocketAddr;

    use kynet::cert::{Certificate, PrivateKey};
    use kynet::error::ConnectionError;
    pub use kynet::wtransport::{WTransportClientOptions, WTransportServerOptions};
    use kynet::Server;

    // Wrapper returning a Connection on accept()
    pub struct WTransportServer(kynet::wtransport::WTransportServer);

    impl WTransportServer {
        pub fn start_on_addr(
            addr: SocketAddr,
            cert_chain: Vec<Certificate>,
            key: PrivateKey,
            options: &WTransportServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server =
                kynet::wtransport::WTransportServer::start_on_addr(addr, cert_chain, key, options)?;
            Ok(Self(server))
        }

        pub fn start(
            port: u16,
            cert_chain: Vec<Certificate>,
            key: PrivateKey,
            options: &WTransportServerOptions,
        ) -> Result<Self, ConnectionError> {
            let server =
                kynet::wtransport::WTransportServer::start(port, cert_chain, key, options)?;
            Ok(Self(server))
        }
    }

    #[async_trait]
    impl super::Server for WTransportServer {
        async fn accept(&self) -> Result<Connection, ConnectionError> {
            let conn = self.0.accept().await?;
            let kyproto = Connection::accept(conn).await?;
            Ok(kyproto)
        }

        async fn accept_with_auth(&self) -> Result<UnauthenticatedConnection, ConnectionError> {
            let conn = self.0.accept().await?;
            let unauth_kyproto = Connection::accept_with_auth(conn).await?;
            Ok(unauth_kyproto)
        }

        fn close(&self, error_code: u32, reason: &str) {
            self.0.close(error_code, reason);
        }

        async fn wait_idle(&self) {
            self.0.wait_idle().await;
        }
    }
}
