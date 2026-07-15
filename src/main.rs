use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, AsyncRead, AsyncWrite, split}, net::TcpListener
};
use tokio_socks::tcp::Socks5Stream;
use anyhow::Result; 

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("Server started");

    loop {
        let (stream, _) = listener.accept().await?;
        println!("connected");

        let client = Socks5Stream::connect("127.0.0.1:1080", "127.0.0.1:1088").await?;
        println!("client connected");

        let (client_read, client_write) = split(client);
        let (stream_read, stream_write) = split(stream);

        let a = async move {
            read_to_write(client_read, stream_write)
                .await
                .unwrap();
        };

        let b = async move {
            read_to_write(stream_read, client_write)
                .await
                .unwrap();
        };

        tokio::select! {
            _ = a => {}
            _ = b => {}
        };
    }
}

async fn read_to_write<R, W>(mut reader: R, mut writer: W) -> Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let mut buf = [0u8; 4096];

        let n = reader.read(&mut buf).await?;

        if n == 0 {
            break;
        }

        let data = &buf[..n];

        writer.write(&data).await?;
    }

    Ok(())
}
