use std::collections::HashMap;
use std::sync::Arc;

use dotenv::dotenv;
#[cfg(target_os = "linux")]
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock as AsyncRwLock;

use nagatoro::{custom_log, kafka_listener};
use nagatoro::config::AppConfig;
use nagatoro::logic::init_db;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    custom_log::do_init();
    dotenv().ok();

    let consumer = Arc::new(AsyncRwLock::new(None));
    let db_pool =  Arc::new(AsyncRwLock::new(HashMap::new()));

    let app_config = AppConfig::get_config().await?;
    init_db(&app_config,Arc::clone(&db_pool)).await?;

    let kafka_handle = tokio::spawn(
        kafka_listener::kafka_listener(Arc::clone(&consumer),Arc::clone(&db_pool))
    );

    kafka_handle.await.unwrap();
    #[cfg(target_os = "linux")]
    {
        let signal_handle = tokio::spawn(signal_listener(Arc::clone(&consumer)));
        signal_handle.await.unwrap();
    }

    Ok(())
}


#[cfg(target_os = "linux")]
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

