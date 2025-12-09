use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use log::info;
use rand::Rng;
use rustls::pki_types;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{self, JoinHandle};

use kymux::ipc::IPCForwardableConnection;
use kyproto::{AudioProtocol, VideoProtocol};

const SERVER_NAME: &str = "kymux_test";
const PORT: u16 = 10000;

#[derive(Debug)]
enum StreamType {
    Video(VideoProtocol),
    Audio(AudioProtocol),
    Input,
}

struct TlsKey {
    cert_chain: Vec<pki_types::CertificateDer<'static>>,
    private_key: pki_types::PrivateKeyDer<'static>,
    certs_store: rustls::RootCertStore,
}

fn gen_keys(server_name: &str) -> TlsKey {
    let names: Vec<String> = vec![server_name.into()];

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

async fn create_connection() -> (IPCForwardableConnection, IPCForwardableConnection) {
    let keys = gen_keys(SERVER_NAME);

    let server_accept = async move {
        let config = kymux::ServerConfig::Quic {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT),
            cert_chain: vec![keys.cert_chain[0].clone()],
            private_key: keys.private_key,
        };

        let server = kymux::Server::new(config).await.unwrap();
        let connection = server.accept().await.unwrap();
        let connection = Arc::new(connection);
        IPCForwardableConnection::new(connection, 9090..9091).await
    };

    let client_connect = async move {
        let config = kymux::ClientConfig::Quic {
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), PORT),
            roots: Some(keys.certs_store),
            server_name: SERVER_NAME.into(),
            certificate_hash: None,
        };

        let connection = kymux::connect(config).await.unwrap();
        let connection = Arc::new(connection);
        IPCForwardableConnection::new(connection, 9091..9092).await
    };

    match tokio::try_join!(server_accept, client_connect) {
        Ok((server, client)) => (server, client),
        _ => {
            panic!("Connect task failed");
        }
    }
}

struct ProducerConfig {
    shutdown_tx: mpsc::UnboundedSender<()>,
    uri: String,
    block_size: usize,
    generate_size: usize,
    completion_rx: oneshot::Receiver<()>,
    write_delay: Option<Duration>,
}

struct ConsumerConfig {
    shutdown_tx: mpsc::UnboundedSender<()>,
    uri: String,
    block_size: usize,
    completion_tx: oneshot::Sender<()>,
    read_delay: Option<Duration>,
}

#[async_trait]
trait StreamIo {
    async fn read(stream: &mut TcpStream) -> Vec<u8>;
    async fn write(stream: &mut TcpStream, data: &[u8]);
}

struct AvStreamIo;

impl AvStreamIo {
    const HEADER_SIZE: usize = 12;
}

#[async_trait]
impl StreamIo for AvStreamIo {
    async fn read(stream: &mut TcpStream) -> Vec<u8> {
        let mut header = [0; Self::HEADER_SIZE];
        stream.read_exact(&mut header).await.unwrap();

        let is_codec: bool = header[0] & 0x80 == 0;
        if is_codec {
            panic!("Unexpected codec header");
        }

        let size = BigEndian::read_u32(&header[8..]);
        let mut payload = BytesMut::zeroed(size as _);
        stream.read_exact(&mut payload).await.unwrap();

        payload.to_vec()
    }

    async fn write(stream: &mut TcpStream, data: &[u8]) {
        let mut header = [0; Self::HEADER_SIZE];

        let pts_and_flags = 1 << 63; // media packet
        BigEndian::write_u64(&mut header[..8], pts_and_flags);
        BigEndian::write_u32(&mut header[8..], data.len() as _);
        stream.write_all(&header).await.unwrap();

        stream.write_all(data).await.unwrap();
    }
}

struct InputStreamIo;

#[async_trait]
impl StreamIo for InputStreamIo {
    async fn read(stream: &mut TcpStream) -> Vec<u8> {
        let mut type_ = [0u8; 1];
        stream.read_exact(&mut type_).await.unwrap();

        let mut buf = [0; 2];
        stream.read_exact(&mut buf).await.unwrap();
        let size = u16::from_be_bytes(buf);

        let mut buf = vec![0u8; size as _];
        stream.read_exact(&mut buf).await.unwrap();

        buf
    }

    async fn write(stream: &mut TcpStream, data: &[u8]) {
        let type_ = 0u8;
        let size = data.len() as u16;

        stream.write_all(&type_.to_be_bytes()).await.unwrap();
        stream.write_all(&size.to_be_bytes()).await.unwrap();
        stream.write_all(&data).await.unwrap();
    }
}

async fn run_producer<T>(producer_config: ProducerConfig)
where
    T: StreamIo,
{
    // Connect to kymux
    let stream = kymux_client::Client::connect(&producer_config.uri)
        .await
        .unwrap();
    let mut stream = stream.into_tcp_stream();

    // Prepare data to send
    let block_count = producer_config.generate_size / producer_config.block_size;
    assert!(producer_config.generate_size % producer_config.block_size == 0);

    let mut block = vec![0u8; producer_config.block_size];
    let mut hasher = Sha1::new();

    // Announce size
    T::write(&mut stream, &producer_config.generate_size.to_be_bytes()).await;

    // Send payload
    for _ in 0..block_count {
        rand::thread_rng().fill(&mut block[..]);
        hasher.update(&block[..]);

        T::write(&mut stream, &block[..]).await;

        if let Some(delay) = producer_config.write_delay {
            tokio::time::sleep(delay).await;
        }
    }

    // Send hash (20 bytes)
    let hash = hasher.finalize().to_vec();
    T::write(&mut stream, &hash[..]).await;

    // Wait completion
    producer_config.completion_rx.await.unwrap();
    producer_config.shutdown_tx.send(()).unwrap();
}

async fn run_consumer<T>(consumer_config: ConsumerConfig)
where
    T: StreamIo,
{
    // Connect to kymux
    let stream = kymux_client::Client::connect(&consumer_config.uri)
        .await
        .unwrap();
    let mut stream = stream.into_tcp_stream();

    // Read size
    let mut buf = Bytes::from(T::read(&mut stream).await);
    let receive_size = buf.get_u64() as usize;
    info!("Prepare to receive {receive_size} bytes");

    // Prepare reception
    let block_count = receive_size / consumer_config.block_size;
    assert!(receive_size % consumer_config.block_size == 0);

    let mut hasher = Sha1::new();

    // Read payload
    for _ in 0..block_count {
        let block = T::read(&mut stream).await;
        hasher.update(&block[..]);

        if let Some(delay) = consumer_config.read_delay {
            tokio::time::sleep(delay).await;
        }
    }

    // Read and compare hash (20 bytes)
    let hash = hasher.finalize().to_vec();

    let announced_hash = T::read(&mut stream).await;
    assert_eq!(hash, announced_hash);

    // Send completion
    consumer_config.completion_tx.send(()).unwrap();
    consumer_config.shutdown_tx.send(()).unwrap();
}

enum Actor {
    Client,
    Server,
}

enum TestStep {
    RunTest {
        // Stream configuration
        endpoint_creator: Actor,
        producer: Actor,
        stream_type: StreamType,
        // Producer configuration
        block_size: usize,
        generate_size: usize,
        write_delay: Option<Duration>,
        // Consumer configuration
        read_delay: Option<Duration>,
    },
    Wait {
        duration: Duration,
    },
}

fn spawn_tasks(
    producer_config: ProducerConfig,
    consumer_config: ConsumerConfig,
    stream_type: StreamType,
) -> Vec<JoinHandle<()>> {
    match stream_type {
        StreamType::Audio(_) | StreamType::Video(_) => {
            let producer_task = task::spawn(run_producer::<AvStreamIo>(producer_config));
            let consumer_task = task::spawn(run_consumer::<AvStreamIo>(consumer_config));

            vec![producer_task, consumer_task]
        }
        StreamType::Input => {
            let producer_task = task::spawn(run_producer::<InputStreamIo>(producer_config));
            let consumer_task = task::spawn(run_consumer::<InputStreamIo>(consumer_config));

            vec![producer_task, consumer_task]
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stress_test() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_timestamp_millis()
        .is_test(true)
        .init();

    // Initialize rustls crypto
    rustls::crypto::ring::default_provider()
        .install_default()
        .unwrap();

    let (mut server, mut client) = create_connection().await;
    info!("QUIC connection ready");

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    let mut tasks = vec![];

    // Run tests with multiple configurations
    let configs = [
        // Run a long test that must survive
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            block_size: 512,
            generate_size: 1024 * 1000,
            write_delay: Some(Duration::from_millis(5)),
            read_delay: Some(Duration::from_millis(10)),
        },
        // Run multiple tests with various combination of (creator, producer)
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Client,
            stream_type: StreamType::Input,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Client,
            stream_type: StreamType::Audio(AudioProtocol::Reliable),
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Server,
            stream_type: StreamType::Audio(AudioProtocol::Reliable),
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Server,
            producer: Actor::Server,
            stream_type: StreamType::Video(VideoProtocol::Reliable),
            block_size: 1024,
            generate_size: 1024 * 1024 * 50,
            write_delay: None,
            read_delay: None,
        },
        // Wait and start tests with read/write delays
        TestStep::Wait {
            duration: Duration::from_secs(1),
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Client,
            stream_type: StreamType::Input,
            block_size: 128,
            generate_size: 128 * 40,
            write_delay: Some(Duration::from_millis(50)),
            read_delay: None,
        },
        TestStep::Wait {
            duration: Duration::from_secs(1),
        },
        TestStep::RunTest {
            endpoint_creator: Actor::Client,
            producer: Actor::Server,
            stream_type: StreamType::Input,
            block_size: 128,
            generate_size: 128 * 40,
            write_delay: None,
            read_delay: Some(Duration::from_millis(50)),
        },
    ];

    for config in configs {
        match config {
            TestStep::RunTest {
                endpoint_creator,
                producer,
                stream_type,
                block_size,
                generate_size,
                write_delay,
                read_delay,
            } => {
                // Create the endpoint
                let (endpoint_registerer, endpoint_connector) = match endpoint_creator {
                    Actor::Client => (&mut client, &mut server),
                    Actor::Server => (&mut server, &mut client),
                };

                let (registerer_uri, connector_uri) = match stream_type {
                    StreamType::Audio(protocol) => {
                        let (endpoint, registerer_uri) = endpoint_registerer
                            .register_and_forward_audio_endpoint(None, protocol)
                            .await
                            .unwrap();
                        let connector_uri = endpoint_connector
                            .connect_and_forward_audio_endpoint(endpoint, protocol)
                            .unwrap();

                        (registerer_uri, connector_uri)
                    }
                    StreamType::Input => {
                        let (endpoint, registerer_uri) = endpoint_registerer
                            .register_and_forward_input_endpoint(None)
                            .await
                            .unwrap();
                        let connector_uri = endpoint_connector
                            .connect_and_forward_input_endpoint(endpoint)
                            .unwrap();

                        (registerer_uri, connector_uri)
                    }
                    StreamType::Video(protocol) => {
                        let (endpoint, registerer_uri) = endpoint_registerer
                            .register_and_forward_video_endpoint(None, protocol)
                            .await
                            .unwrap();
                        let connector_uri = endpoint_connector
                            .connect_and_forward_video_endpoint(endpoint, protocol)
                            .unwrap();

                        (registerer_uri, connector_uri)
                    }
                };

                // Map the producer and the consumer to the correct actors
                let (producer_uri, consumer_uri) = match (endpoint_creator, producer) {
                    (Actor::Client, Actor::Client) | (Actor::Server, Actor::Server) => {
                        (registerer_uri, connector_uri)
                    }
                    (Actor::Client, Actor::Server) | (Actor::Server, Actor::Client) => {
                        (connector_uri, registerer_uri)
                    }
                };

                // Create the configurations
                let (completion_tx, completion_rx) = oneshot::channel();

                let producer_config = ProducerConfig {
                    shutdown_tx: shutdown_tx.clone(),
                    uri: producer_uri,
                    block_size,
                    generate_size,
                    completion_rx,
                    write_delay,
                };

                let consumer_config = ConsumerConfig {
                    shutdown_tx: shutdown_tx.clone(),
                    uri: consumer_uri,
                    block_size,
                    completion_tx,
                    read_delay,
                };

                // Run tasks
                let mut test_tasks = spawn_tasks(producer_config, consumer_config, stream_type);
                tasks.append(&mut test_tasks);
            }
            TestStep::Wait { duration } => {
                tokio::time::sleep(duration).await;
            }
        }
    }

    // Run a batch of concurrent tests
    for i in 0..20 {
        let (endpoint_registerer, endpoint_connector, stream_type) = if i % 2 == 0 {
            (&mut client, &mut server, StreamType::Input)
        } else {
            (
                &mut server,
                &mut client,
                StreamType::Video(VideoProtocol::Reliable),
            )
        };

        // Create the endpoint
        let (producer_uri, consumer_uri) = match stream_type {
            StreamType::Input => {
                let (endpoint, registerer_uri) = endpoint_registerer
                    .register_and_forward_input_endpoint(None)
                    .await
                    .unwrap();
                let connector_uri = endpoint_connector
                    .connect_and_forward_input_endpoint(endpoint)
                    .unwrap();

                (registerer_uri, connector_uri)
            }
            StreamType::Video(protocol) => {
                let (endpoint, registerer_uri) = endpoint_registerer
                    .register_and_forward_video_endpoint(None, protocol)
                    .await
                    .unwrap();
                let connector_uri = endpoint_connector
                    .connect_and_forward_video_endpoint(endpoint, protocol)
                    .unwrap();

                (registerer_uri, connector_uri)
            }
            StreamType::Audio(_) => panic!("Unexpected stream type {stream_type:?}"),
        };

        // Create the configurations
        let (completion_tx, completion_rx) = oneshot::channel();

        let block_size = 1024;
        let generate_size = 1024 * 1024 * 10;

        let producer_config = ProducerConfig {
            shutdown_tx: shutdown_tx.clone(),
            uri: producer_uri,
            block_size,
            generate_size,
            completion_rx,
            write_delay: None,
        };

        let consumer_config = ConsumerConfig {
            shutdown_tx: shutdown_tx.clone(),
            uri: consumer_uri,
            block_size,
            completion_tx,
            read_delay: None,
        };

        // Run tasks
        let mut test_tasks = spawn_tasks(producer_config, consumer_config, stream_type);
        tasks.append(&mut test_tasks);
    }

    // Wait for all shutdown sender to be dropped.
    // It means that all tasks are finished.
    drop(shutdown_tx);

    let mut task_count = tasks.len();
    info!("Started {task_count} tasks");

    while let Some(_) = shutdown_rx.recv().await {
        task_count -= 1;
        info!("Task stopped. Remaining: {task_count}");
    }

    // All tasks can be joined now
    for t in tasks {
        t.await.unwrap();
    }
}
