use std::collections::HashSet;
use std::sync::Arc;
use log::{info, warn};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use tokio::sync::RwLock as AsyncRwLock;
use crate::logic;
use crate::config::AppConfig;

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

pub type LoggingConsumer = StreamConsumer<CustomContext>;

pub async fn kafka_listener(
    consumer: Arc<AsyncRwLock<Option<LoggingConsumer>>>,
) {
    let app_config = AppConfig::get_config().await.unwrap();
    let topic_str = app_config.kafka_topic.clone();
    let context = CustomContext;

    let kafka_consumer:LoggingConsumer  = ClientConfig::new()
        .set("group.id", &app_config.kafka_job_group_id)
        .set("bootstrap.servers", &app_config.kafka_brokers)
        .set("enable.auto.commit", &app_config.kafka_enable_auto_commit)
        .set("acks", &app_config.kafka_acks)
        .set("max.poll.interval.ms", &app_config.kafka_max_poll_interval_ms)
        .set("session.timeout.ms", &app_config.kafka_session_timeout_ms)
        .create_with_context(context)
        .expect("Consumer creation failed");

    kafka_consumer
        .subscribe(&[&topic_str])
        .expect("Can't subscribe to specified topics");


    consumer.write().await.replace(kafka_consumer);
    let mut custom_consumer_context = CustomConsumerContext {
        processed_offsets: HashSet::new(),
        max_offsets_capacity: 20000,
    };
    loop {
        match consumer.read().await.as_ref().unwrap().recv().await {
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
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                let offset = m.offset();
                if custom_consumer_context.processed_offsets.contains(&(String::from(&topic_str), offset)) {
                    info!("Skipping duplicate message at offset:{}", offset);
                    continue; // jump
                }
                if let Ok(_) = logic::process_message(payload).await {
                    info!("process message ok");
                }
                custom_consumer_context.add_offset(String::from(&topic_str), offset);
                consumer.read().await.as_ref().unwrap().commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
