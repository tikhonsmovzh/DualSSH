use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split}, net::{
        TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf},
    },
};
use tokio_socks::tcp::Socks5Stream;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;

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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let ssh_1_stream: TcpStream;
    let ssh_2_stream: TcpStream;

    match &cli.command {
        Commands::Server { target } => {
            ssh_1_stream = TcpListener::bind("127.0.0.1:7878").await?.accept().await?.0;
            ssh_2_stream = TcpListener::bind("127.0.0.1:7078").await?.accept().await?.0;
        }

        Commands::Client { listener } => {
            ssh_1_stream = Socks5Stream::connect("127.0.0.1:1080", "127.0.0.1:7878")
                .await?
                .into_inner();
            ssh_2_stream = Socks5Stream::connect("127.0.0.1:1085", "127.0.0.1:7078")
                .await?
                .into_inner();
        }
    }

    let (ssh_1_read, ssh_1_write) = ssh_1_stream.into_split();
    let (ssh_2_read, ssh_2_write) = ssh_2_stream.into_split();

    let arc_ssh_1_writer = Arc::new(Mutex::new(ssh_1_write));
    let arc_ssh_2_writer = Arc::new(Mutex::new(ssh_2_write));

    if let Commands::Client { listener } = &cli.command {
        let mut connections: u8 = 0;
        let mut writes_map: HashMap<u8, OwnedWriteHalf> = HashMap::new();

        let listener = TcpListener::bind(listener).await?;

        loop {
            let client_stream = listener.accept().await?.0;

            let (reader, writer) = client_stream.into_split();

            tokio::spawn(reader_to_writers(reader, arc_ssh_1_writer.clone(), arc_ssh_2_writer.clone(), connections));

            writes_map.insert(connections, writer);

            connections += 1;
        }
    }

    Ok(())
}

async fn reader_to_writers(mut reader: OwnedReadHalf, mut writer1: Arc<Mutex<OwnedWriteHalf>>, mut writer2: Arc<Mutex<OwnedWriteHalf>>, connection_id: u8) -> Result<()>{
    let mut buf = [0u8;  1024];

    loop {
        let n = reader.read(&mut buf).await?;

        if n == 0 {
            break;
        }

        let mut w1 = writer1.lock().await;

        w1.write(&buf[..n]).await?;

        let mut w2 = writer2.lock().await;

        w2.write(&buf[..n]).await?;
    }

    Ok(())
}