use anyhow::Result;
use clap::{Parser, Subcommand};

mod reconected_stream;
use crate::reconected_stream::{ServerTcpConnection, TcpConnection};

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

    match &cli.command {
        Commands::Server { target: _ } => {

        },
        Commands::Client { listener: _ } => {

        },
    }

    Ok(())
}