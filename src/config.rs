use config::{Config, Environment, File, FileFormat};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub kafka_brokers: String,
    pub kafka_topic: String,
    pub kafka_job_group_id: String,
    pub kafka_auto_offset_reset: String,
    pub kafka_enable_auto_commit: String,
    pub kafka_acks: String,
    pub kafka_max_poll_records: String,
    pub kafka_max_poll_interval_ms: String,
    pub kafka_session_timeout_ms: String,
    pub database_nagatoro_url: String,
    pub database_tukinashi_url: String,
}

pub fn get_env_file() -> String {
    #[cfg(feature = "pdt")]
    {
        "conf/cfg_prod.yaml".to_string()
    }
    #[cfg(feature = "dev")]
    {
        "conf/cfg_dev.yaml".to_string()
    }
    #[cfg(feature = "test")]
    {
        "conf/cfg_test.yaml".to_string()
    }
}

impl AppConfig {
    pub async fn get_config() -> Result<AppConfig, Box<dyn std::error::Error>>{
        let config_file = get_env_file();
        let settings = Config::builder()
            .add_source(File::new(&config_file, FileFormat::Yaml))
            .add_source(Environment::with_prefix("app").separator("_"))
            .build()
            .unwrap();
            let app_config = settings.clone().try_deserialize::<AppConfig>();
        match app_config {
            Ok(v) => {
                println!("fetch conf ok");
                Ok(v)
            }
            Err(e) => {
                println!("fetch conf failed");
                Err(Box::try_from(e).unwrap())
            }
        }
    }
}

