use log::{error, info};
use nagatoro::logic;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Ok(_) = logic::init().await {
        info!("init success");
    } else {
        error!("init error");
    }
}


