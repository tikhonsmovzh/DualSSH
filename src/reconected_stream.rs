use anyhow::{Ok, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_socks::tcp::Socks5Stream;

pub trait TcpConnection {
    async fn write(&mut self, buf: &[u8]) -> Result<usize>;
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
}

pub struct ServerTcpConnection {
    listener: Arc<Mutex<TcpListener>>,
    stream: Arc<Mutex<TcpStream>>,
    is_connected: Arc<Mutex<bool>>,
}

impl ServerTcpConnection {
    pub async fn new(target_addr: String) -> Result<Self> {
        let listener = TcpListener::bind(target_addr).await?;

        let stream = listener.accept().await?.0;

        Ok(ServerTcpConnection {
            listener: Arc::new(Mutex::new(listener)),
            stream: Arc::new(Mutex::new(stream)),
            is_connected: Arc::new(Mutex::new(true)),
        })
    }

    async fn reconnect(&mut self) {
        println!("reconnecting...");

        let mut connection_guardian = self.is_connected.lock().await;
        *connection_guardian = false;

        let is_connected_clone = self.is_connected.clone();
        let strem_clone = self.stream.clone();
        let listener_clone = self.listener.clone();

        tokio::spawn(async move {
            let mut guardian = strem_clone.lock().await;

            *guardian = listener_clone.lock().await.accept().await.unwrap().0;

            let mut connection_guardian = is_connected_clone.lock().await;
            *connection_guardian = true;

            println!("reconnected sucsesfull");
        });
    }
}

impl TcpConnection for ServerTcpConnection {
    async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if !self.is_connected.lock().await.clone() {
            return Ok(0);
        }

        let n = self.stream.lock().await.write(buf).await?;

        if n != buf.len() {
            println!("connection to client closed on write");

            self.reconnect().await
        }

        Ok(n)
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if !self.is_connected.lock().await.clone() {
            return Ok(0);
        }

        let n = self.stream.lock().await.read(buf).await?;

        if n == 0 {
            println!("connection to client closed on read");

            self.reconnect().await
        }

        Ok(n)
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

    async fn reconnect(&mut self) {
        println!("reconnecting...");

        let mut connection_guardian = self.is_connected.lock().await;
        *connection_guardian = false;

        let is_connected_clone = self.is_connected.clone();
        let stream_clone = self.stream.clone();
        let proxy_addr_clone = self.proxy_addr.clone();
        let target_addr_clone = self.target_addr.clone();

        tokio::spawn(async move {
            let stream =
                Socks5Stream::connect(proxy_addr_clone.as_str(), target_addr_clone.as_str())
                    .await
                    .unwrap();

            let mut guardian = stream_clone.lock().await;
            *guardian = stream;

            let mut connection_guardian = is_connected_clone.lock().await;
            *connection_guardian = true;

            println!("reconnected sucsesfull");
        });
    }
}

impl TcpConnection for ClientTcpConnection {
    async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if !self.is_connected.lock().await.clone() {
            return Ok(0);
        }

        let n = self.stream.lock().await.write(buf).await?;

        if n != buf.len() {
            println!("connection to server closed on write");

            self.reconnect().await
        }

        Ok(n)
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if !self.is_connected.lock().await.clone() {
            return Ok(0);
        }

        let n = self.stream.lock().await.read(buf).await?;

        if n == 0 {
            println!("connection to server closed on read");

            self.reconnect().await
        }

        Ok(n)
    }
}

pub enum AnyConnection {
    Server(ServerTcpConnection),
    Client(ClientTcpConnection),
}

impl AnyConnection {
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        match self {
            AnyConnection::Server(c) => c.write(&buf).await,
            AnyConnection::Client(c) => c.write(&buf).await,
        }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        match self {
            AnyConnection::Server(c) => c.read(buf).await,
            AnyConnection::Client(c) => c.read(buf).await,
        }
    }
}
