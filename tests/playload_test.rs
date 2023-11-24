use std::collections::HashMap;
use log::info;

use nagatoro::{cmn, db};
use nagatoro::logic::process_message;

#[tokio::test]
pub async fn t2(){
    info!("start test ");
    let s = r#"
    {
    "database":"nakatoro",
    "beforeData":{
        "currPayAmount":1059.67,
        "proType":0,
        "id":2192645,
        "operatorId":999
    },
    "type":"update",
    "tableName":"T_POOL",
    "afterData":{
        "currPayAmount":1059.67,
        "proType":0,
        "id":2192645,
        "operatorId":2
    }
    "#;
    let url = "mysql://nagatoro:123@127.0.0.1:3306/nakatoro?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&useSSL=false";
    let url2 = "mysql://nagatoro:123@127.0.0.1:3306/tukinashi?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&autoReconnect=true&useSSL=false";
    let mut pools = HashMap::new();
    if let Ok(_v) = db::init(&url).await {
        pools.insert(cmn::PoolKey::NakatoroPool, _v);
    }

    if let Ok(_v) = db::init(&url2).await {
        pools.insert(cmn::PoolKey::TukinashiPool, _v);
    }
    process_message(s,&pools).await.unwrap();
}

