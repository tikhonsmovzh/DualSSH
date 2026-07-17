use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_socks::tcp::Socks5Stream;

#[derive(Parser)]
#[command(name = "mode")]
#[command(about = "Dual ssh protocol", version = "1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Server {
        #[arg(short, long, value_name = "ADDR")]
        target: String,
    },
    Client {
        #[arg(short, long, value_name = "ADDR")]
        listener: String,
    },
}

const BUF_SIZE: usize = 64 * 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let ssh_1_stream: TcpStream;
    let ssh_2_stream: TcpStream;

    match &cli.command {
        Commands::Server { target: _ } => {
            ssh_1_stream = TcpListener::bind("127.0.0.1:7878").await?.accept().await?.0;
            ssh_2_stream = TcpListener::bind("127.0.0.1:7078").await?.accept().await?.0;

            println!("client connected");
        }

        Commands::Client { listener: _ } => {
            ssh_1_stream = Socks5Stream::connect("127.0.0.1:1080", "127.0.0.1:7878")
                .await?
                .into_inner();
            ssh_2_stream = Socks5Stream::connect("127.0.0.1:1085", "127.0.0.1:7078")
                .await?
                .into_inner();

            println!("connected to server");
        }
    }

    let (mut ssh_1_read, ssh_1_write) = ssh_1_stream.into_split();
    let (mut ssh_2_read, ssh_2_write) = ssh_2_stream.into_split();

    let arc_ssh_1_writer = Arc::new(Mutex::new(ssh_1_write));
    let arc_ssh_2_writer = Arc::new(Mutex::new(ssh_2_write));

    match cli.command {
        Commands::Client { listener } => {
            let writes_map: Arc<Mutex<HashMap<u8, OwnedWriteHalf>>> =
                Arc::new(Mutex::new(HashMap::new()));
            let readers_map: Arc<Mutex<HashMap<u8, tokio::task::JoinHandle<Result<(), Error>>>>> =
                Arc::new(Mutex::new(HashMap::new()));

            let writes_map_copy = writes_map.clone();
            let readers_map_copy = readers_map.clone();

            tokio::spawn(async move {
                let listener = TcpListener::bind(listener).await.unwrap();

                loop {
                    let client_stream = listener.accept().await.unwrap().0;
                    println!("new connection");

                    let (reader, writer) = client_stream.into_split();

                    let mut connection = 0;

                    let mut map = writes_map_copy.lock().await;

                    for key in 0..=u8::MAX {
                        if !map.contains_key(&key) {
                            connection = key;
                            break;
                        }
                    }

                    map.insert(connection, writer);

                    println!("connection id {}", connection);

                    let writes_map_copy_1 = writes_map_copy.clone();
                    let readers_map_copy_1 = readers_map_copy.clone();

                    readers_map_copy.lock().await.insert(
                        connection,
                        tokio::spawn(reader_to_writers(
                            reader,
                            arc_ssh_1_writer.clone(),
                            arc_ssh_2_writer.clone(),
                            connection,
                            async move {
                                writes_map_copy_1.lock().await.remove(&connection);
                                readers_map_copy_1.lock().await.remove(&connection);
                            },
                        )),
                    );

                    println!("start transfer connection id {}", connection);
                }
            });

            let mut buf1 = [1u8; BUF_SIZE];
            let mut buf2 = [1u8; BUF_SIZE];

            let mut current_packet = 0;

            loop {
                let r1 = ssh_1_read.read_exact(&mut buf1[0..4]);
                let r2 = ssh_2_read.read_exact(&mut buf2[0..4]);

                let buf: &mut [u8; BUF_SIZE];
                let reader: &mut OwnedReadHalf;

                tokio::select! {
                    _ = r1 => {
                        buf = &mut buf1;
                        reader = &mut ssh_1_read;
                    }
                    _ = r2 => {
                        buf = &mut buf2;
                        reader = &mut ssh_2_read;
                    }
                }

                let packet = buf[1];

                let connection = buf[0];

                if packet != 0 {
                    let dif = packet - current_packet;

                    if dif > 0 && dif < 20 {
                        current_packet = packet;
                        current_packet %= 255;
                    } else {
                        continue;
                    }
                }
                else {
                    writes_map.lock().await.remove(&connection);

                    let mut readers_map_mut = readers_map.lock().await;

                    if readers_map_mut.contains_key(&connection) {
                        readers_map_mut[&connection].abort();

                        readers_map_mut.remove(&connection);
                    }

                    println!("remove connection id {}", connection);

                    continue;
                }

                let data_size = u16::from_be_bytes(buf[2..4].try_into()?) as usize;

                reader.read_exact(&mut buf[4..(data_size + 4)]).await?;

                writes_map
                    .lock()
                    .await
                    .get_mut(&connection)
                    .unwrap()
                    .write(&buf[4..(data_size + 4)])
                    .await?;
            }
        }

        Commands::Server { target } => {
            let writes_map: Arc<Mutex<HashMap<u8, OwnedWriteHalf>>> =
                Arc::new(Mutex::new(HashMap::new()));

            let readers_map: Arc<Mutex<HashMap<u8, tokio::task::JoinHandle<Result<(), Error>>>>> =
                Arc::new(Mutex::new(HashMap::new()));

            let mut buf1 = [1u8; BUF_SIZE];
            let mut buf2 = [1u8; BUF_SIZE];

            let mut current_packet = 0;

            loop {
                let r1 = ssh_1_read.read_exact(&mut buf1[0..4]);
                let r2 = ssh_2_read.read_exact(&mut buf2[0..4]);

                let buf: &mut [u8; BUF_SIZE];
                let reader: &mut OwnedReadHalf;

                tokio::select! {
                    _ = r1 => {
                        buf = &mut buf1;
                        reader = &mut ssh_1_read;
                    }
                    _ = r2 => {
                        buf = &mut buf2;
                        reader = &mut ssh_2_read;
                    }
                }

                let packet = buf[1];

                let connection = buf[0];

                if packet != 0 {
                    let dif = packet - current_packet;

                    if dif > 0 && dif < 20 {
                        current_packet = packet;
                        current_packet %= 255;
                    } else {
                        continue;
                    }
                } else {
                    writes_map.lock().await.remove(&connection);

                    let mut readers_map = readers_map.lock().await;

                    if readers_map.contains_key(&connection) {
                        readers_map[&connection].abort();
                        readers_map.remove(&connection);
                    }

                    println!("remove connection id {}", connection);

                    continue;
                }

                if !writes_map.lock().await.contains_key(&connection) {
                    let client_stream = match TcpStream::connect(&target).await {
                        Ok(c) => c,
                        Err(e) => {
                            println!("connection to client with error {}", e);
                            arc_ssh_1_writer
                                .lock()
                                .await
                                .write(&[connection, 0, 0, 0])
                                .await?;
                            arc_ssh_2_writer
                                .lock()
                                .await
                                .write(&[connection, 0, 0, 0])
                                .await?;
                            continue;
                        }
                    };

                    println!("new connection id {}", connection);

                    let (reader, writer) = client_stream.into_split();

                    let writers_map_copy = writes_map.clone();
                    let readers_map_copy = readers_map.clone();

                    readers_map.lock().await.insert(
                        connection,
                        tokio::spawn(reader_to_writers(
                            reader,
                            arc_ssh_1_writer.clone(),
                            arc_ssh_2_writer.clone(),
                            connection,
                            async move {
                                writers_map_copy.lock().await.remove(&connection);
                                readers_map_copy.lock().await.remove(&connection);
                            },
                        )),
                    );

                    println!("start transfer connection id {}", connection);

                    writes_map.lock().await.insert(connection, writer);
                }

                let data_size = u16::from_be_bytes([buf[2], buf[3]]) as usize;

                reader.read_exact(&mut buf[4..(data_size + 4)]).await?;

                writes_map
                    .lock()
                    .await
                    .get_mut(&connection)
                    .unwrap()
                    .write(&buf[4..(data_size + 4)])
                    .await?;
            }
        }
    }
}

async fn reader_to_writers<F>(
    mut reader: OwnedReadHalf,
    writer1: Arc<Mutex<OwnedWriteHalf>>,
    writer2: Arc<Mutex<OwnedWriteHalf>>,
    connection_id: u8,
    on_connection_close: F,
) -> Result<()>
where
    F: Future,
{
    let mut buf = [1u8; BUF_SIZE];
    let mut packet_counter = 0;

    loop {
        let n: u16 = reader.read(&mut buf[4..(BUF_SIZE)]).await? as u16;

        if n == 0 {
            println!("stop transfer connection id {}", connection_id);

            writer1
                .lock()
                .await
                .write_all(&[connection_id, 0, 0, 0])
                .await?;

            on_connection_close.await;

            break;
        }

        let nb = n.to_be_bytes();

        buf[0] = connection_id;
        buf[2] = nb[0];
        buf[3] = nb[1];

        packet_counter %= 255;
        packet_counter += 1;
        buf[1] = packet_counter;

        writer1
            .lock()
            .await
            .write_all(&buf[..(n as usize + 4)])
            .await?;

        writer2
            .lock()
            .await
            .write_all(&buf[..(n as usize + 4)])
            .await?;
    }

    Ok(())
}
