use clap::{AppSettings, Clap};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Result,
};
use tokio_amqp::LapinTokioExt;

/// Basic receiver and sender example.
#[derive(Debug, Clap)]
#[clap(name = "RabbitMQ - Tutorial 01", setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(long, default_value = "127.0.0.1")]
    addr: String,
    #[clap(long, default_value = "5672")]
    port: u32,
    /// Specify if the mode is `receive` or `send` if false
    #[clap(short, long)]
    receive: bool,
}

async fn send(channel: Channel) -> Result<()> {
    let payload = "Hello World!";

    let confirm = channel
        .basic_publish(
            "",
            "hello",
            BasicPublishOptions::default(),
            payload.as_bytes().to_vec(),
            BasicProperties::default(),
        )
        .await?
        .await?;

    println!("[x] Sent {}\nconfirm: {:?}", payload, confirm);
    Ok(())
}

async fn receive(channel: Channel) -> Result<()> {
    let consumer = channel
        .basic_consume(
            "hello",
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut it = consumer.into_iter();
    println!(" [*] Waiting for messages. To exit press CTRL+C");
    while let Some(delivery) = it.next() {
        match delivery {
            Ok((_channel, delivery)) => {
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("basic_ack");
                let msg = std::str::from_utf8(&delivery.data).expect("invalid string");
                println!(" [x] Received {}", msg);
            }
            Err(error) => {
                println!("Error caught in consumer: {}", error)
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let addr = format!("amqp://{}:{}/%2f", opts.addr, opts.port);
    let conn = Connection::connect(&addr, ConnectionProperties::default().with_tokio()).await?;
    let channel = conn.create_channel().await?;

    let _queue = channel
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    if opts.receive {
        receive(channel).await?;
    } else {
        send(channel).await?;
    }

    Ok(())
}
