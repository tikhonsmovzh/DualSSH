use anyhow::{Ok, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_socks::tcp::Socks5Stream;

pub trait TcpConnection {
    async fn write(self, buf: &[u8]) -> Result<usize>;
    async fn read(self, buf: &mut [u8]) -> Result<usize>;
    async fn is_connected(self) -> Result<bool>;
}

pub struct ServerTcpConnection {
    listener: TcpListener,
    stream: Arc<Mutex<TcpStream>>,
    is_connected: Arc<Mutex<bool>>,
}

impl ServerTcpConnection {
    pub async fn new(target_addr: String) -> Result<Self> {
        let listener = TcpListener::bind(target_addr).await?;

        let stream = listener.accept().await?.0;

        Ok(ServerTcpConnection {
            listener,
            stream: Arc::new(Mutex::new(stream)),
            is_connected: Arc::new(Mutex::new(true)),
        })
    }

    async fn reconnect(self) {
        println!("reconnecting...");

        let mut connection_guardian = self.is_connected.lock().await;
        *connection_guardian = false;

        let is_connected_clone = self.is_connected.clone();

        tokio::spawn(async move {
            let mut guardian = self.stream.lock().await;

            *guardian = self.listener.accept().await.unwrap().0;

            let mut connection_guardian = is_connected_clone.lock().await;
            *connection_guardian = true;

            println!("reconnected sucsesfull");
        });
    }
}

impl TcpConnection for ServerTcpConnection {
    async fn write(self, buf: &[u8]) -> Result<usize> {
        let n = self.stream.lock().await.write(buf).await?;

        if n != buf.len() {
            println!("connection to client closed on write");

            self.reconnect().await
        }

        Ok(n)
    }

    async fn read(self, buf: &mut [u8]) -> Result<usize> {
        let n = self.stream.lock().await.read(buf).await?;

        if n == 0 {
            println!("connection to client closed on read");

            self.reconnect().await
        }

        Ok(n)
    }

    async fn is_connected(self) -> Result<bool> {
        Ok(self.is_connected.lock().await.clone())
    }
}

pub struct ClientTcpConnection {
    stream: Arc<Mutex<Socks5Stream<TcpStream>>>,
    is_connected: Arc<Mutex<bool>>,
    proxy_addr: String,
    target_addr: String,
}

impl ClientTcpConnection {
    pub async fn new(proxy_addr: String, target_addr: String) -> Result<Self> {
        Ok(ClientTcpConnection {
            stream: Arc::new(Mutex::new(
                Socks5Stream::connect(proxy_addr.as_str(), target_addr.as_str()).await?,
            )),
            is_connected: Arc::new(Mutex::new(true)),
            proxy_addr,
            target_addr,
        })
    }

    async fn reconnect(self) {
        println!("reconnecting...");

        let mut connection_guardian = self.is_connected.lock().await;
        *connection_guardian = false;

        let is_connected_clone = self.is_connected.clone();

        tokio::spawn(async move {
            let stream = Socks5Stream::connect(self.proxy_addr.as_str(), self.target_addr.as_str())
                .await
                .unwrap();

            let mut guardian = self.stream.lock().await;
            *guardian = stream;

            let mut connection_guardian = is_connected_clone.lock().await;
            *connection_guardian = true;

            println!("reconnected sucsesfull");
        });
    }
}

impl TcpConnection for ClientTcpConnection {
    async fn write(self, buf: &[u8]) -> Result<usize> {
        let n = self.stream.lock().await.write(buf).await?;

        if n != buf.len() {
            println!("connection to server closed on write");

            self.reconnect().await
        }

        Ok(n)
    }

    async fn read(self, buf: &mut [u8]) -> Result<usize> {
        let n = self.stream.lock().await.read(buf).await?;

        if n == 0 {
            println!("connection to server closed on read");

            self.reconnect().await
        }

        Ok(n)
    }

    async fn is_connected(self) -> Result<bool> {
        Ok(self.is_connected.lock().await.clone())
    }
}
