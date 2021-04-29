use clap::{AppSettings, Clap};
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind, Result,
};
use tokio_amqp::LapinTokioExt;

const EXCHANGE_NAME: &str = "topic_logs";

/// In this tutorial we use the "publish/subscribe" with topic exchange type
#[derive(Debug, Clap)]
#[clap(name = "RabbitMQ - Tutorial 05", setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(default_value = "anonymous.info")]
    routing_key: String,
    /// Log message
    #[clap(default_value = "Hello World!")]
    msg: String,
    #[clap(long, default_value = "127.0.0.1")]
    addr: String,
    #[clap(long, default_value = "5672")]
    port: u32,
    /// Specify if the mode is `receive` or `send` if false
    #[clap(short, long)]
    receiver: bool,
}

async fn emit_log_topic(msg: String, routing_key: String, channel: Channel) -> Result<()> {
    let payload = msg.as_bytes().to_vec();

    let confirm = channel
        .basic_publish(
            EXCHANGE_NAME,
            &routing_key,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?
        .await?;

    println!(
        "[x] Sent \"{}:{}\"\nconfirm: {:?}",
        routing_key, msg, confirm
    );
    Ok(())
}

async fn receive_logs_topic(channel: Channel, binding_keys: String) -> Result<()> {
    let binding_keys = binding_keys
        .split_whitespace()
        .into_iter()
        .collect::<Vec<_>>();

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

    for binding_key in binding_keys {
        channel
            .queue_bind(
                queue_name,
                EXCHANGE_NAME,
                binding_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
    }

    let consumer = channel
        .basic_consume(
            queue_name,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!(" [*] Waiting for logs. To exit press CTRL+C");

    let mut it = consumer.into_iter();
    while let Some(delivery) = it.next() {
        match delivery {
            Ok((_ch, delivery)) => {
                let msg = std::str::from_utf8(&delivery.data).expect("invalid string");
                println!(" [x] \"{}:{}\"", delivery.routing_key, msg);
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("basic_ack");
            }
            Err(error) => {
                println!("Error caught in consumer: {}", error)
            }
        };
    }

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
            EXCHANGE_NAME,
            ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    if opts.receiver {
        receive_logs_topic(channel, opts.routing_key).await?;
    } else {
        emit_log_topic(opts.msg, opts.routing_key, channel).await?;
    }

    Ok(())
}
