use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tokio::sync::Mutex;

mod reconected_stream;
use crate::{
    arguments::{Cli, Commands},
    connection::handle_connection,
    reconected_stream::{AnyConnection, ClientTcpConnection, ServerTcpConnection},
};

mod arguments;
mod connection;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let ssh_connection_1: AnyConnection;
    let ssh_connection_2: AnyConnection;

    match &cli.command {
        Commands::Server { target: _ } => {
            println!("waiting client");

            ssh_connection_1 = AnyConnection::Server(
                ServerTcpConnection::new("127.0.0.1:7878".to_string()).await?,
            );
            ssh_connection_2 = AnyConnection::Server(
                ServerTcpConnection::new("127.0.0.1:7078".to_string()).await?,
            );

            println!("client connected");
        }
        Commands::Client { listener: _ } => {
            println!("connecting to server");

            ssh_connection_1 = AnyConnection::Client(
                ClientTcpConnection::new(
                    "127.0.0.1:1080".to_string(),
                    "127.0.0.1:7878".to_string(),
                )
                .await?,
            );
            ssh_connection_2 = AnyConnection::Client(
                ClientTcpConnection::new(
                    "127.0.0.1:1085".to_string(),
                    "127.0.0.1:7078".to_string(),
                )
                .await?,
            );

            println!("connected to server");
        }
    }

    println!("start protocol");

    handle_connection(
        cli.command,
        Arc::new(Mutex::new(ssh_connection_1)),
        Arc::new(Mutex::new(ssh_connection_2)),
    )
    .await?;

    Ok(())
}
