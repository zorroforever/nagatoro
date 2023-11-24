use std::collections::{HashMap, HashSet};

use log::{info, warn};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use sqlx::{MySql, Pool};

use crate::config::AppConfig;
use crate::{cmn, logic};

#[derive(Default)]
struct CustomConsumerContext {
    processed_offsets: HashSet<(String, i64)>,
    max_offsets_capacity: usize,
}

impl CustomConsumerContext {

    fn add_offset(&mut self, topic: String, offset: i64) {
        if self.processed_offsets.len() > self.max_offsets_capacity {
            self.clean_up_offsets();
        }
        self.processed_offsets.insert((topic, offset));
    }

    fn clean_up_offsets(&mut self) {
        self.processed_offsets.clear();
    }
}

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

pub async fn init(
    app_config: &AppConfig,
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut custom_consumer_context = CustomConsumerContext {
        processed_offsets: HashSet::new(),
        max_offsets_capacity: 20000,
    };
    let topic_str = app_config.kafka_topic.clone();
    let context = CustomContext;
    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", &app_config.kafka_job_group_id)
        .set("bootstrap.servers", &app_config.kafka_brokers)
        .set("enable.auto.commit", &app_config.kafka_enable_auto_commit)
        .set("acks", &app_config.kafka_acks)
        .set("max.poll.interval.ms", &app_config.kafka_max_poll_interval_ms)
        .set("session.timeout.ms", &app_config.kafka_session_timeout_ms)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&topic_str])
        .expect("Subscribe failed");
    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                let _key = m.key().map(|k| k.to_owned());
                let _value = m.payload().map(|p| p.to_owned());
                let offset = m.offset();
                info!("key: {}, payload: {}, topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      String::from_utf8_lossy(m.key().unwrap()), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if custom_consumer_context.processed_offsets.contains(&(String::from(&topic_str), offset)) {
                    info!("Skipping duplicate message at offset:{}", offset);
                    continue; // jump
                }
                logic::process_message(&payload, db_pool).await.unwrap();
                custom_consumer_context.add_offset(String::from(&topic_str), offset);
                consumer
                    .commit_message(&m, CommitMode::Async)
                    .expect("Failed to commit offset");
                info!("End process at offset:{}", offset);
            }
        };
    }
}