use std::collections::HashMap;

use log::{error, info, warn};
use serde_json::Value;
use sqlx::{MySql, Pool};

use crate::{cmn, db, entity};
use crate::cmn::PoolKey;
use crate::config::AppConfig;

pub async fn init_db(
    app_config: &AppConfig,
) -> Result<HashMap<PoolKey,Pool<MySql>>, Box<dyn std::error::Error + Send + Sync>> {
    let mut pools = HashMap::new();
    if let Ok(_v) = db::init(&app_config.database_nagatoro_url).await {
        pools.insert(cmn::PoolKey::NakatoroPool, _v);
    } else {
        error!("nagatoro url init error");
        return Ok(pools);
    }
    if let Ok(_v) = db::init(&app_config.database_tukinashi_url).await {
        pools.insert(cmn::PoolKey::TukinashiPool, _v);
    } else {
        error!("tukinashi url init error");
        return Ok(pools);
    }
    Ok(pools)
}
pub async fn process_message(
    play_load: &str,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    let json_playload: serde_json::Value = serde_json::from_str(play_load).unwrap();
    let f_type = json_playload["type"].as_str().unwrap();
    let f_table_name = json_playload["tableName"].as_str().unwrap();
    let f_bf = &json_playload["beforeData"];
    let f_af = &json_playload["afterData"];
    let app_config = AppConfig::get_config().await?;
    let db_pool = init_db(&app_config).await?;
    match f_table_name {
        "T_POOL" => {
            if let Ok(_) = process_pool(f_type, f_bf, f_af, &db_pool).await{
                info!("process_pool ok");
            } else {
                info!("process_pool error");
            }
        }
        "T_ACTION" => {
            if let Ok(_) = process_action(f_type, f_bf, f_af, &db_pool).await {
                info!("process_action ok");
            } else {
                info!("process_action error");
            }
        }
        _ => { info!("No need process!") }
    }
    Ok(())
}

async fn process_action(
    f_type: &str,
   _j_bf: &serde_json::Value,
   _j_af: &serde_json::Value,
   db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    if let Ok(_) = do_flush_action_data(db_pool, _j_bf,f_type).await {
        info!("do_flush_action_data bf is ok");
    }
    Ok(())
}


async fn do_flush_action_data(
    db_pool: &HashMap<PoolKey, Pool<MySql>>,
    obj: &Value,
    f_type: &str,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    let mut _app_id =String::new() ;
    if let Some(v)=cmn::get_str_from_json(obj,"cAppId"){
        _app_id = v;
    }else {
        warn!("do_flush_action_data_by_app_id is none");
        return Ok(());
    }
    if let Ok(mut gaibu_show_data) = db::get_gaibu_show_data_by_app_id(db_pool,&_app_id).await{
        if let Some(_collection_action_id) = obj["id"].as_i64() {
            if let Ok(_) = do_flush_action_data_by_app_id(db_pool,&mut gaibu_show_data,_collection_action_id,f_type).await{
                info!("do_flush_action_data_by_app_id is ok");
            }
        }
        if let Ok(_) = db::update_gaibu_show_data_by_entity_v2(db_pool,&gaibu_show_data).await{
            info!("update_gaibu_show_data_by_entity is ok");
        }
    } else {
        info!("app id:{} do not have pool data",&_app_id);
    }

    Ok(())
}


pub async fn process_pool(
    f_type: &str,
    _j_bf: &serde_json::Value,
    _j_af: &serde_json::Value,
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    if f_type == cmn::BIN_LOG_TYPE_INS {
        do_create_data(f_type, db_pool, _j_af).await?;
    } else if f_type == cmn::BIN_LOG_TYPE_UPD {
        do_update_data(f_type, db_pool, _j_af).await?;
    } else if f_type == cmn::BIN_LOG_TYPE_DEL {
        do_delete_data(f_type,db_pool, _j_bf).await?;
    } else {
        info!("No need process!")
    }
    Ok(())
}

pub async fn do_create_data(
    f_type: &str,
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
    _obj_after: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    if let Ok(_) = do_update_data(f_type,db_pool,_obj_after).await {
        info!("create data ok");
    }
    Ok(())
}

pub async fn do_update_data(
    f_type: &str,
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
    _obj_after: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    let _after_operator_id = _obj_after["operatorId"].as_i64().unwrap_or(-1_i64);
    let _after_collection_id = _obj_after["id"].as_i64().unwrap_or(-1_i64);
    if _after_collection_id == -1 {
        error!("null collectionId id");
        return Ok(());
    }
    if let Ok((mut _v, _size)) = db::get_gaibu_show_data_by_collection_id(db_pool, _after_collection_id).await {
        if _size > 0 {
            if let Some(mut _v_one) = _v.get(0) {
                let id = &_v_one.id.unwrap_or(-1_i64);
                //  after operator is not out, remove the data on show
                if *id != -1 {
                    if let Ok(_) = db::remove_gaibu_show_data_by_id(db_pool, id).await {
                        info!("remove_gaibu_show_data_by_id ok");
                    }
                    return Ok(());
                }
            }
        } else {
            if let Ok(one_init) = do_collection_gaibu_show_data_data_for_insert(f_type, db_pool, _obj_after).await{
                if let Ok(_) = db::insert_gaibu_show_data_by_entity(db_pool, &one_init).await {
                    info!("insert_gaibu_show_data_by_entity is ok");
                }
            }
        }
    };

    Ok(())
}


async fn do_collection_gaibu_show_data_data_for_insert(
    f_type: &str,
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
    obj: &serde_json::Value,
) -> Result<entity::GaibuShowData, Box<dyn std::error::Error+ Send + Sync>> {
    let mut result = entity::GaibuShowData::default();
    let to_operator_id = obj["operatorId"].as_i64().unwrap_or(-1_i64);
    if to_operator_id <= 0 {
        error!("null operator id");
        return Ok(result);
    }
    // collection id
    result.collection_id = obj["id"].as_i64();
    // product code
    result.product_code = obj["proType"].as_i64();
    // operator id
    result.operator_id = Some(to_operator_id);
    // set action data by appId
    if let Ok(_) =
         result.do_flush_action_data_by_app_id(
             db_pool,
             &entity::Action::default(),
             &f_type,
    ).await {
         info!("set action data by appId,ok");
    };

    Ok(result)
}


pub async fn do_delete_data(
    _type: &str,
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
    _obj_before: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    if let Some(_collection_id) = _obj_before["id"].as_i64() {
        if let Ok(_) = db::delete_gaibu_show_data_by_collection_id(db_pool,_collection_id).await {
            info!("delete_gaibu_show_data_by_collection_id ok");
        }
    }
    Ok(())
}

pub async fn do_flush_action_data_by_app_id(
    db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
    gaibu_show_data: &mut entity::GaibuShowData,
    collection_action_id: i64,
    bin_log_type: &str,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>> {
    let func = "do_flush_action_data_by_app_id";
    let app_id = gaibu_show_data.app_id.clone().unwrap_or("".to_string());
    if app_id == "" {
        warn!("{}||null app id.",&func);
        ()
    }
    if bin_log_type == cmn::BIN_LOG_TYPE_DEL {
        let ext_id =collection_action_id;
        match db::get_max_action_by_app_id_and_ext_id(db_pool, &app_id, ext_id).await {
            Ok((collection_action, cnt)) => {
                if cnt <= 0 {
                    gaibu_show_data.action_code = None;
                    gaibu_show_data.action_time = None;
                    gaibu_show_data.action_memo = None;
                } else {
                    gaibu_show_data.action_code = collection_action.action_code;
                    gaibu_show_data.action_time = collection_action.action_time;
                    gaibu_show_data.action_memo = collection_action.memo;
                }
            }
            Err(_e) => {
                warn!("{}||collection action error.",&func);
                ()
            }
        }
    } else {
        match db::get_max_collection_action_by_app_id(db_pool, &app_id).await {
            Ok((collection_action, cnt)) => {
                if cnt > 0 {
                    gaibu_show_data.action_code = collection_action.action_code;
                    gaibu_show_data.action_time = collection_action.action_time;
                    gaibu_show_data.action_memo = collection_action.memo;
                }
            }
            Err(_e) => {
                warn!("do_flush_action_data_by_app_id||collection action error.");
            }
        }
    }
    Ok(())
}
