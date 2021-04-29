use clap::{AppSettings, Clap};
use lapin::{
    message::DeliveryResult,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind, Result,
};
use tokio_amqp::LapinTokioExt;

/// In this tutorial we use the "publish/subscribe" pattern to broadcast logs to all
/// available receivers.
#[derive(Debug, Clap)]
#[clap(name = "RabbitMQ - Tutorial 03", setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(default_value = "info: Hello World!")]
    msg: String,
    #[clap(long, default_value = "127.0.0.1")]
    addr: String,
    #[clap(long, default_value = "5672")]
    port: u32,
    /// Specify if the mode is `receive` or `send` if false
    #[clap(short, long)]
    receiver: bool,
}

async fn emit_log(msg: String, channel: Channel) -> Result<()> {
    let payload = msg.as_bytes().to_vec();

    let confirm = channel
        .basic_publish(
            "logs",
            "",
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?
        .await?;

    println!("[x] Sent {}\nconfirm: {:?}", msg, confirm);
    Ok(())
}

async fn receive_logs(channel: Channel) -> Result<()> {
    let result = channel
        .queue_declare(
            "",
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    let queue_name = result.name().as_str();
    channel
        .queue_bind(
            queue_name,
            "logs",
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let consumer = channel
        .basic_consume(
            queue_name,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    consumer.set_delegate(|delivery: DeliveryResult| async move {
        match delivery {
            Ok(delivery) => {
                if let Some((_ch, delivery)) = delivery {
                    let msg = std::str::from_utf8(&delivery.data).expect("invalid string");
                    println!(" [x] {}", msg);
                    delivery
                        .ack(BasicAckOptions::default())
                        .await
                        .expect("basic_ack");
                }
            }
            Err(error) => {
                println!("Error caught in consumer: {}", error)
            }
        };
    })?;

    let mut it = consumer.into_iter();
    println!(" [*] Waiting for messages. To exit press CTRL+C");
    while let Some(_delivery) = it.next() {}

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let addr = format!("amqp://{}:{}/%2f", opts.addr, opts.port);
    let conn = Connection::connect(&addr, ConnectionProperties::default().with_tokio()).await?;
    let channel = conn.create_channel().await?;

    channel
        .exchange_declare(
            "logs",
            ExchangeKind::Fanout,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    if opts.receiver {
        receive_logs(channel).await?;
    } else {
        emit_log(opts.msg, channel).await?;
    }

    Ok(())
}
