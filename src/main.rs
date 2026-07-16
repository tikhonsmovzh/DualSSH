use anyhow::Result;
use clap::{Parser, Subcommand, command};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split}, net::{
        TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf},
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

    if let Commands::Client { listener } = &cli.command {
        let mut connections: u8 = 0;

        let listener = TcpListener::bind(listener).await?;

        loop {
            let client_stream = listener.accept().await?.0;

            let (client_read, client_write) = client_stream.into_split();
            let (ssh_1_read, ssh_1_write) = &ssh_1_stream.into_split();
            let (ssh_2_read, ssh_2_write) = &ssh_2_stream.into_split();


            tokio::spawn(rider_to_writers(client_read, ssh_1_write, ssh_2_write));
            tokio::spawn(writers_to_reader(ssh_1_read, ssh_2_read, client_write));
        }
    }

    Ok(())
}

async fn rider_to_writers(reader: OwnedReadHalf, writer1: &OwnedWriteHalf, writter2: &OwnedWriteHalf) -> Result<()>
{
    

    Ok(())
}

async fn writers_to_reader(reader1: &OwnedReadHalf, reader2: &OwnedReadHalf, writter: OwnedWriteHalf) -> Result<()>
{
    

    Ok(())
}