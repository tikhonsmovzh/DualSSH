use anyhow::{Error, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::Mutex,
};

use crate::{arguments::Commands, reconected_stream::AnyConnection};

const BUF_SIZE: usize = 32 * 1024;

pub async fn handle_connection(
    state: Commands,
    ssh_1_connection: Arc<Mutex<AnyConnection>>,
    ssh_2_connection: Arc<Mutex<AnyConnection>>,
) -> Result<()> {
    let writers_map: Arc<Mutex<HashMap<u8, OwnedWriteHalf>>> = Arc::new(Mutex::new(HashMap::new()));

    let readers_map: Arc<Mutex<HashMap<u8, tokio::task::JoinHandle<Result<(), Error>>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    if let Commands::Client {
        listener: listener_addr,
    } = state.clone()
    {
        let writers_map_clone = writers_map.clone();
        let readers_map_clone = readers_map.clone();
        let ssh_1_connection_clone = ssh_1_connection.clone();
        let ssh_2_connection_clone = ssh_2_connection.clone();

        let packet_counter = Arc::new(Mutex::new(0u32));

        tokio::spawn(async move {
            let listenert = TcpListener::bind(listener_addr).await.unwrap();

            loop {
                let stream = listenert.accept().await.unwrap().0;

                println!("new connection");

                let (stream_reader, stream_writer) = stream.into_split();

                let mut connection = 0;

                let mut map_guardian = writers_map_clone.lock().await;

                for key in 0..=u8::MAX {
                    if !map_guardian.contains_key(&key) {
                        connection = key;
                        break;
                    }
                }

                map_guardian.insert(connection, stream_writer);

                println!("connection id: {}", connection);

                let mut packet_counter_guardian = packet_counter.lock().await;

                *packet_counter_guardian %= u32::MAX;
                *packet_counter_guardian += 1;

                let packet_buf = packet_counter_guardian.to_be_bytes();
                let data = [
                    packet_buf[0],
                    packet_buf[1],
                    packet_buf[2],
                    packet_buf[3],
                    connection,
                    2,
                    0,
                    0,
                ];

                ssh_1_connection_clone
                    .lock()
                    .await
                    .write(&data)
                    .await
                    .unwrap();
                ssh_2_connection_clone
                    .lock()
                    .await
                    .write(&data)
                    .await
                    .unwrap();

                let readers_map_clone_1 = readers_map_clone.clone();
                let writers_map_clone_1 = writers_map_clone.clone();

                readers_map_clone.lock().await.insert(
                    connection,
                    tokio::spawn(reader_to_writers(
                        stream_reader,
                        ssh_1_connection_clone.clone(),
                        ssh_2_connection_clone.clone(),
                        connection,
                        packet_counter.clone(),
                        async move {
                            readers_map_clone_1.lock().await.remove(&connection);
                            writers_map_clone_1.lock().await.remove(&connection);

                            println!("stop transfer {}", connection);
                        },
                    )),
                );

                println!("start transfer {}", connection);
            }
        });
    }

    let mut buf1 = [1u8; BUF_SIZE];
    let mut buf2 = [1u8; BUF_SIZE];
    let ssh_1_connection_clone = ssh_1_connection.clone();
    let ssh_2_connection_clone = ssh_2_connection.clone();
    let mut last_packet = 0;

    let packet_counter = Arc::new(Mutex::new(0u32));

    loop {
        let mut ssh_1_guardian = ssh_1_connection_clone.lock().await;
        let r1 = ssh_1_guardian.read(&mut buf1[..8]);
        let mut ssh_2_guardian = ssh_2_connection_clone.lock().await;
        let r2 = ssh_2_guardian.read(&mut buf2[..8]);

        let buf: &mut [u8];
        let data_size: usize;

        tokio::select! {
            _ = r1 => {
                data_size = u16::from_be_bytes([buf1[6], buf1[7]]) as usize;

                let mut ssh_guardian = ssh_1_connection_clone.lock().await;
                ssh_guardian.read(&mut buf1[8..(8 + data_size)]).await?;

                buf = &mut buf1;
            }
            _ = r2 => {
                data_size = u16::from_be_bytes([buf2[6], buf2[7]]) as usize;

                let mut ssh_guardian = ssh_2_connection_clone.lock().await;
                ssh_guardian.read(&mut buf2[8..(8 + data_size)]).await?;

                buf = &mut buf2;
            }
        }

        let packet = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

        let dif = packet - last_packet;

        if dif > 0 && dif < 5 {
            last_packet = packet;
            last_packet %= u32::MAX as usize;

            let connection = buf[4];
            let info = buf[5];

            if let Commands::Server { target } = state.clone() {
                let writers_map_clone = writers_map.clone();
                let readers_map_clone = readers_map.clone();

                if info == 2 {
                    println!("new connection");

                    let ssh_1_connection_clone_1 = ssh_1_connection_clone.clone();
                    let ssh_2_connection_clone_1 = ssh_2_connection_clone.clone();
                    let packet_counter_clone = packet_counter.clone();

                    tokio::spawn(async move {
                        let stream = TcpStream::connect(target).await.unwrap();

                        let (reader, writer) = stream.into_split();

                        writers_map_clone.lock().await.insert(connection, writer);

                        let readers_map_clone_1 = readers_map_clone.clone();
                        let writers_map_clone_1 = writers_map_clone.clone();

                        readers_map_clone.lock().await.insert(
                            connection,
                            tokio::spawn(reader_to_writers(
                                reader,
                                ssh_1_connection_clone_1.clone(),
                                ssh_2_connection_clone_1.clone(),
                                connection,
                                packet_counter_clone.clone(),
                                async move {
                                    readers_map_clone_1.lock().await.remove(&connection);
                                    writers_map_clone_1.lock().await.remove(&connection);

                                    println!("stop transfer by world {}", connection);
                                },
                            )),
                        );

                        println!("start transfer {}", connection);
                    });

                    break;
                }
            }

            if info == 3 {
                writers_map.lock().await.remove(&connection);
                let mut readers_map = readers_map.lock().await;

                if readers_map.contains_key(&connection) {
                    readers_map[&connection].abort();
                    readers_map.remove(&connection);
                }

                println!("stop transfer by channel {}", connection);
            } else if info == 1 {
                writers_map
                    .lock()
                    .await
                    .get_mut(&connection)
                    .unwrap()
                    .write(&buf[8..(8 + data_size)])
                    .await?;
            }
        }
    }

    Ok(())
}

async fn reader_to_writers<F>(
    mut reader: OwnedReadHalf,
    ssh_1_connection: Arc<Mutex<AnyConnection>>,
    ssh_2_connection: Arc<Mutex<AnyConnection>>,
    connection_id: u8,
    packet_counter: Arc<Mutex<u32>>,
    on_connection_close: F,
) -> Result<()>
where
    F: Future,
{
    let mut buf = [1u8; BUF_SIZE];

    loop {
        let n = reader.read(&mut buf[8..BUF_SIZE]).await?;

        let mut packet_counter_guardian = packet_counter.lock().await;

        *packet_counter_guardian %= u32::MAX;
        *packet_counter_guardian += 1;

        let packet_buf = packet_counter_guardian.to_be_bytes();

        if n == 0 {
            let data = [
                packet_buf[0],
                packet_buf[1],
                packet_buf[2],
                packet_buf[3],
                connection_id,
                3,
                0,
                0,
            ];

            ssh_1_connection.lock().await.write(&data).await.unwrap();
            ssh_2_connection.lock().await.write(&data).await.unwrap();

            on_connection_close.await;

            break;
        }

        let data_size_buf = (n as u16).to_be_bytes();

        buf[0] = packet_buf[0];
        buf[1] = packet_buf[1];
        buf[2] = packet_buf[2];
        buf[3] = packet_buf[3];
        buf[4] = connection_id;
        buf[5] = 1;
        buf[6] = data_size_buf[0];
        buf[7] = data_size_buf[1];

        ssh_1_connection.lock().await.write(&buf[..(n + 7)]).await.unwrap();
        ssh_2_connection.lock().await.write(&buf[..(n + 7)]).await.unwrap();
    }

    Ok(())
}
