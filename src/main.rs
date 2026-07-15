use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:7878").await?;
    println!("Server started");

    loop {
        let (mut stream, socket) = listener.accept().await?;
        println!("connected");

        let client = TcpStream::connect("127.0.0.1:1080").await?;
        println!("client connected");

        let (client_read, client_write) = client.into_split();
        let (stream_read, stream_write) = stream.into_split();

        let a = async {
            read_to_write(client_read, stream_write, "read".to_string())
                .await
                .unwrap();
        };

        let b = async {
            read_to_write(stream_read, client_write, "write".to_string())
                .await
                .unwrap();
        };

        tokio::select! {
            _ = a => {}
            _ = b => {}
        };
    }

    Ok(())
}

async fn read_to_write(
    mut read: OwnedReadHalf,
    mut write: OwnedWriteHalf,
    str: String,
) -> std::io::Result<()> {
    loop {
        println!("run {str}");
        let mut buf = [0u8; 4096];

        let n = read.read(&mut buf).await?;

        println!("{n} {str}");

        if n == 0 {
            break;
        }

        let data = &buf[..n];

        write.write(&data).await?;
    }

    Ok(())
}
