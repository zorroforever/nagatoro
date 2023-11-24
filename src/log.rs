use std::env;
use log4rs::append::console::ConsoleAppender;

use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log::LevelFilter;
use sqlx::types::chrono::Local;

pub fn init() {

    let args: Vec<String> = env::args().collect();
    let console_appender = ConsoleAppender::builder().encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new(
        "{d(%Y-%m-%d %H:%M:%S%.3f)} {l}  [{T}-{i}] {m}{n}")))
        .build();

    let now = Local::now().format("%Y-%m-%d");

    let file_appender = FileAppender::builder()
        .encoder(Box::new(log4rs::encode::pattern::PatternEncoder::new(
            "{d(%Y-%m-%d %H:%M:%S%.3f)} {l}  [{T}-{i}] {m}{n}",
        )))
        .build(format!("var/log/rust/nagatoro_{}.log.{}",args[1],now))
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("console", Box::new(console_appender)))
        .appender(Appender::builder().build("file", Box::new(file_appender)))
        .build(
            Root::builder()
                .appender("console")
                .appender("file")
                .build(LevelFilter::Debug),
        )
        .unwrap();

    log4rs::init_config(config).unwrap();

}
