use std::time::Duration;

use clap::{AppSettings, Clap};
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Result,
};
use tokio::time::sleep;
use tokio_amqp::LapinTokioExt;

/// This tutorial focuses on 2 things:
///
/// 1. Making queues durable, so they can survive if RabbitMQ service is restarted.
///
/// 2. Set the channel `prefetch_count` to control how many tasks a worker can have
///    at any time. Setting this to 1 will dispatch tasks only to workers that are
///    not busy.
#[derive(Debug, Clap)]
#[clap(name = "RabbitMQ - Tutorial 02", setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(default_value = "Hello World!")]
    msg: String,
    #[clap(long, default_value = "127.0.0.1")]
    addr: String,
    #[clap(long, default_value = "5672")]
    port: u32,
    /// Specify if the mode is `receive` or `send` if false
    #[clap(short, long)]
    worker: bool,
}

async fn new_task(msg: String, channel: Channel) -> Result<()> {
    let payload = msg.as_bytes().to_vec();

    let confirm = channel
        .basic_publish(
            "",
            "task_queue",
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default().with_delivery_mode(2), // make message persistent
        )
        .await?
        .await?;

    println!("[x] sent {}\nconfirm: {:?}", msg, confirm);
    Ok(())
}

async fn worker(channel: Channel) -> Result<()> {
    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let consumer = channel
        .basic_consume(
            "task_queue",
            "rust_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut it = consumer.into_iter();
    println!(" [*] Waiting for messages. To exit press CTRL+C");
    while let Some(delivery) = it.next() {
        match delivery {
            Ok((_ch, delivery)) => {
                let msg = std::str::from_utf8(&delivery.data).expect("invalid string");
                println!(" [x] Received {}", msg);
                let sleep_duration = msg.chars().filter(|o| o == &'.').count();
                sleep(Duration::from_secs(sleep_duration as u64)).await;
                println!(" [x] Done");
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("basic_ack");
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
            "task_queue",
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    if opts.worker {
        worker(channel).await?;
    } else {
        new_task(opts.msg, channel).await?;
    }

    Ok(())
}
