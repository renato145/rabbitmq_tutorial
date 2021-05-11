use clap::{AppSettings, Clap};
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Result,
};
use tokio_amqp::LapinTokioExt;
use uuid::Uuid;

const QUEUE_NAME: &str = "rpc_queue";
const ROUTING_KEY: &str = "rpc_queue";

/// RPC server/client for calculating fib(n)
#[derive(Debug, Clap)]
#[clap(name = "RabbitMQ - Tutorial 06", setting = AppSettings::ColoredHelp)]
struct Opts {
    /// To calculate fib(n)
    #[clap(default_value = "30")]
    n: i32,
    #[clap(long, default_value = "127.0.0.1")]
    addr: String,
    #[clap(long, default_value = "5672")]
    port: u32,
    /// Specify if the mode is `receive` or `send` if false
    #[clap(short, long)]
    server: bool,
}

async fn rpc_client(n: i32, channel: Channel) -> Result<()> {
    let payload = n.to_string().as_bytes().to_vec();
    println!(" [x] Requesting fib({})", n);

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
    let correlation_id = Uuid::new_v4().to_string();

    channel
        .basic_publish(
            "",
            ROUTING_KEY,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default()
                .with_reply_to(queue_name.into())
                .with_correlation_id(correlation_id.into()),
        )
        .await?
        .await?;

    let consumer = channel
        .basic_consume(
            queue_name,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut it = consumer.into_iter();
    while let Some(delivery) = it.next() {
        match delivery {
            Ok((_ch, delivery)) => {
                let res: i32 = std::str::from_utf8(&delivery.data)
                    .expect("invalid input")
                    .parse()
                    .expect("invalid input");
                println!(" [.] Got {}", res);
                break;
            }
            Err(error) => {
                println!("Error caught in consumer: {}", error)
            }
        };
    }

    Ok(())
}

fn fib(n: i32) -> i32 {
    match n {
        0 => 0,
        1 => 1,
        n => fib(n - 1) + fib(n - 2),
    }
}

async fn rpc_server(channel: Channel) -> Result<()> {
    channel
        .queue_declare(
            QUEUE_NAME,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let consumer = channel
        .basic_consume(
            QUEUE_NAME,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!(" [*] Awaiting RPC requests");

    let mut it = consumer.into_iter();
    while let Some(delivery) = it.next() {
        match delivery {
            Ok((ch, delivery)) => {
                if delivery.data.is_empty() {
                    return Ok(());
                }
                let n = std::str::from_utf8(&delivery.data)
                    .expect("invalid input")
                    .parse()
                    .expect("invalid input");
                println!(" [] fib({})", n);
                let res = fib(n);
                let payload = res.to_string().as_bytes().to_vec();

                let reply_to = delivery
                    .properties
                    .reply_to()
                    .as_ref()
                    .expect("Error reading `reply_to`");

                let correlation_id = delivery
                    .properties
                    .correlation_id()
                    .clone()
                    .expect("Error reading `correlation_id`");

                ch.basic_publish(
                    "",
                    reply_to.as_str(),
                    BasicPublishOptions::default(),
                    payload,
                    BasicProperties::default().with_correlation_id(correlation_id),
                )
                .await?
                .await?;

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

    if opts.server {
        rpc_server(channel).await?;
    } else {
        rpc_client(opts.n, channel).await?;
    }

    Ok(())
}
