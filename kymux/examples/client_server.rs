use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use kymux::ipc::IPCForwardableConnection;
use rustls::pki_types;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

const KYMUX_PORT: u16 = 9090;
const SERVER_NAME: &str = "kymux_example";

struct TlsKey {
    cert_chain: Vec<pki_types::CertificateDer<'static>>,
    private_key: pki_types::PrivateKeyDer<'static>,
    certs_store: rustls::RootCertStore,
}

fn gen_keys(server_name: &str) -> TlsKey {
    let names = vec![server_name.into()];

    // rcgen: Generate Cert + Key
    let (cert_der, private_key) = {
        let cert = rcgen::generate_simple_self_signed(names).unwrap();

        let cert_der = cert.cert.der().clone();
        let private_key = pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());

        (cert_der, private_key)
    };

    let mut certs_store = rustls::RootCertStore::empty();
    certs_store.add(cert_der.clone()).unwrap();

    TlsKey {
        cert_chain: vec![cert_der],
        private_key,
        certs_store,
    }
}

async fn send_playload(tx: &mut OwnedWriteHalf, payload: u64) {
    let type_ = 0u8;
    let size = 8u16;

    tx.write_all(&type_.to_be_bytes()).await.unwrap();
    tx.write_all(&size.to_be_bytes()).await.unwrap();
    tx.write_all(&payload.to_be_bytes()).await.unwrap();
}

async fn recv_payload(rx: &mut OwnedReadHalf) -> u64 {
    let mut type_ = [0u8; 1];
    rx.read_exact(&mut type_).await.unwrap();

    let mut buf = [0; 2];
    rx.read_exact(&mut buf).await.unwrap();
    let size = u16::from_be_bytes(buf);
    assert_eq!(size, 8);

    let mut buf = [0u8; 8];
    rx.read_exact(&mut buf).await.unwrap();
    u64::from_be_bytes(buf)
}

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .format_timestamp_millis()
        .init();

    // Initialize rustls crypto
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    let keys = gen_keys(SERVER_NAME);

    // Server
    let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), KYMUX_PORT);

    let server_config = kymux::ServerConfig {
        addr: server_addr,
        cert_chain: keys.cert_chain.clone(),
        private_key: keys.private_key,
    };

    let server_accept_task = async move {
        let server = kymux::Server::new(server_config).await.unwrap();
        let connection = server.accept().await.unwrap();
        let connection = Arc::new(connection);
        IPCForwardableConnection::new(connection, 9091..9092)
            .await
            .unwrap()
    };

    // Client
    let client_addr = format!("127.0.0.1:{port}", port = KYMUX_PORT)
        .parse()
        .unwrap();

    let client_config = kymux::ClientConfig::Quic {
        addr: client_addr,
        roots: Some(keys.certs_store),
        server_name: SERVER_NAME.into(),
        certificate_hash: None,
    };

    let client = kymux::connect(client_config);

    // Connect
    let (server_ret, client_ret) = tokio::join!(server_accept_task, client);
    let mut server = server_ret;
    let client = client_ret.unwrap();
    let client = Arc::new(client);
    let mut client = IPCForwardableConnection::new(client, 9092..9093)
        .await
        .unwrap();

    println!("Connected");

    let (endpoint, server_addr_endpoint1) = server
        .register_and_forward_input_endpoint(None)
        .await
        .unwrap();
    let client_addr_endpoint1 = client.connect_and_forward_input_endpoint(endpoint).unwrap();
    println!("Endpoint {endpoint:X} registered");

    let (endpoint2, server_addr_endpoint2) = server
        .register_and_forward_input_endpoint(None)
        .await
        .unwrap();
    let client_addr_endpoint2 = client
        .connect_and_forward_input_endpoint(endpoint2)
        .unwrap();

    // Create host client
    let a = tokio::task::spawn(async move {
        println!("Connect clients");

        let host_client = kymux_client::Client::connect(&server_addr_endpoint1);
        let client_client = kymux_client::Client::connect(&client_addr_endpoint1);

        let (host_client_ret, client_client_ret) = tokio::join!(host_client, client_client);
        let host_client = host_client_ret.unwrap();
        let client_client = client_client_ret.unwrap();

        println!("Clients ready");
        let (_host_rx, mut host_tx) = host_client.split();
        let (mut client_rx, _client_tx) = client_client.split();

        for offset in 0..100 {
            let payload = 0x0123456789u64 + offset;

            send_playload(&mut host_tx, payload).await;
            let rx_payload = recv_payload(&mut client_rx).await;
            println!("Got payload 0x{rx_payload:X}");
            assert_eq!(rx_payload, payload);

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Create host client 2
    let b = tokio::task::spawn(async move {
        println!("Connect clients 2");

        let host_client2 = kymux_client::Client::connect(&server_addr_endpoint2);
        let client_client2 = kymux_client::Client::connect(&client_addr_endpoint2);

        let (host_client2_ret, client_client2_ret) = tokio::join!(host_client2, client_client2);
        let host_client2 = host_client2_ret.unwrap();
        let client_client2 = client_client2_ret.unwrap();

        println!("Clients ready");
        let (_host2_rx, mut host2_tx) = host_client2.split();
        let (mut client2_rx, _client2_tx) = client_client2.split();

        for offset in 0..100 {
            let payload = 0xABCDEF6789u64 + offset;

            send_playload(&mut host2_tx, payload).await;
            let rx_payload = recv_payload(&mut client2_rx).await;
            println!("Got payload 0x{rx_payload:X}");
            assert_eq!(rx_payload, payload);

            tokio::time::sleep(Duration::from_millis(120)).await;
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Endpoint 3
    let (endpoint3, server_addr_endpoint3) = server
        .register_and_forward_input_endpoint(None)
        .await
        .unwrap();
    let client_addr_endpoint3 = client
        .connect_and_forward_input_endpoint(endpoint3)
        .unwrap();

    // Create host client 3
    let c = tokio::task::spawn(async move {
        println!("Connect clients 3");

        let host_client = kymux_client::Client::connect(&server_addr_endpoint3);
        let client_client = kymux_client::Client::connect(&client_addr_endpoint3);

        let (host_client_ret, client_client_ret) = tokio::join!(host_client, client_client);
        let host_client2 = host_client_ret.unwrap();
        let client_client2 = client_client_ret.unwrap();

        println!("Clients ready");
        let (_host_rx, mut host_tx) = host_client2.split();
        let (mut client_rx, _client_tx) = client_client2.split();

        for offset in 0..100 {
            let payload = 0xFFFF0000u64 + offset;

            send_playload(&mut host_tx, payload).await;
            let rx_payload = recv_payload(&mut client_rx).await;
            println!("Got payload 0x{rx_payload:X}");
            assert_eq!(rx_payload, payload);

            tokio::time::sleep(Duration::from_millis(130)).await;
        }
    });

    let _ = tokio::join!(a, b, c);
}
