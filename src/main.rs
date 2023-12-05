use std::sync::Arc;

use dotenv::dotenv;
use log::info;
use rdkafka::consumer::{CommitMode, Consumer};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock as AsyncRwLock;

use nagatoro::{custom_log, kafka_listener};
use nagatoro::kafka_listener::LoggingConsumer;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    custom_log::do_init();
    dotenv().ok();
    let consumer = Arc::new(AsyncRwLock::new(None));
    let consumer_clone = Arc::clone(&consumer);
    let consumer_clone2 = Arc::clone(&consumer);
    let kafka_handle = tokio::spawn(
        kafka_listener::kafka_listener(consumer_clone)
    );
    let signal_handle = tokio::spawn(signal_listener(consumer_clone2));
    kafka_handle.await.unwrap();
    signal_handle.await.unwrap();
    Ok(())
}



async fn signal_listener(consumer: Arc<AsyncRwLock<Option<LoggingConsumer>>>) {
    let mut signals = signal(SignalKind::terminate()).unwrap();

    while let Some(()) = signals.recv().await {
        info!("Received shutdown message");
        if let Some(_c) = consumer.read().await.as_ref() {
            _c
                .commit_consumer_state(CommitMode::Sync)
                .unwrap_or_default();
        }
        std::process::exit(0);
    }
}

