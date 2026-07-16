use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split},
    net::{TcpListener, TcpStream},
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

const BUF_SIZE: usize = 1024;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { target } => {
            let listener1 = TcpListener::bind("127.0.0.1:7878").await?;
            let listener2 = TcpListener::bind("127.0.0.1:7078").await?;
            println!("server started");

            loop {
                let (stream1, _) = listener1.accept().await?;
                let (stream2, _) = listener2.accept().await?;
                println!("new connection");

                let client = TcpStream::connect(&target).await?;
                println!("client connected");

                let (stream_in, stream_out) = split(client);
                let (ssh1_in, ssh1_out) = split(stream1);
                let (ssh2_in, ssh2_out) = split(stream2);

                let client_server = async {
                    client_to_server(stream_in, ssh1_out, ssh2_out)
                        .await
                        .unwrap();
                };

                let server_client = async {
                    server_to_client(stream_out, ssh1_in, ssh2_in)
                        .await
                        .unwrap();
                };

                println!("start transfer");

                tokio::select! {
                    _ = server_client => {}
                    _ = client_server => {}
                };

                println!("connection closed");
            }
        }

        Commands::Client { listener } => {
            let listener = TcpListener::bind(listener).await?;
            println!("client started");

            loop {
                let (stream, _) = listener.accept().await?;
                println!("new connection");

                let ssh1 = Socks5Stream::connect("127.0.0.1:1080", "127.0.0.1:7878").await?;
                let ssh2 = Socks5Stream::connect("127.0.0.1:1085", "127.0.0.1:7078").await?;
                println!("server connected");

                let (stream_in, stream_out) = split(stream);
                let (ssh1_in, ssh1_out) = split(ssh1);
                let (ssh2_in, ssh2_out) = split(ssh2);

                let client_server = async {
                    client_to_server(stream_in, ssh1_out, ssh2_out)
                        .await
                        .unwrap();
                };

                let server_client = async {
                    server_to_client(stream_out, ssh1_in, ssh2_in)
                        .await
                        .unwrap();
                };

                println!("start transfer");

                tokio::select! {
                    _ = server_client => {}
                    _ = client_server => {}
                };

                println!("connection closed");
            }
        }
    }

    // Ok(())
}

async fn client_to_server<R, W>(mut reader: R, mut writer1: W, mut writer2: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = [0u8; BUF_SIZE];
    let mut packet_counter: u8 = 0;

    loop {
        let n = reader.read(&mut buf[..(BUF_SIZE - 1)]).await?;

        println!("out {}", n);

        if n == 0 {
            break;
        }

        packet_counter %= 255;
        packet_counter += 1;
        buf[n] = packet_counter;

        let data = &buf[..(n + 1)];

        let w1 = writer1.write_all(&data);
        let w2 = writer2.write_all(&data);

        w1.await?;
        w2.await?;
    }

    Ok(())
}

async fn server_to_client<R, W>(mut writer: W, mut reader1: R, mut reader2: R) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf1 = [0u8; BUF_SIZE];
    let mut buf2 = [0u8; BUF_SIZE];

    let mut current_packed: u8 = 0;

    loop {
        let r1 = reader1.read(&mut buf1);
        let r2 = reader2.read(&mut buf2);

        tokio::select! {
            n1 = r1 => {
                let n = n1?;

                println!("in 1 {}", n);

                if n == 0 {
                    break;
                }

                let packet = buf1[n - 1];
                let dif = packet - current_packed;

                if dif > 0 && dif < 30 {
                    current_packed = packet;

                    current_packed %= 255;

                    writer.write_all(&buf1[..(n - 1)]).await?;
                }
            }
            n2 = r2 => {
                let n = n2?;

                println!("in 2 {}", n);

                if n == 0 {
                    break;
                }

                let packet = buf2[n - 1];
                let dif = packet - current_packed;

                if dif > 0 && dif < 30 {
                    current_packed = packet;

                    current_packed %= 255;

                    writer.write_all(&buf2[..(n - 1)]).await?;
                }
            }
        };
    }

    Ok(())
}
