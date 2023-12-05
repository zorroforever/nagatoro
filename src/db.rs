use std::collections::HashMap;

use log::error;
use sqlx::{MySql, Pool};
use sqlx::mysql::MySqlPoolOptions;

use crate::{cmn, entity};
use crate::cmn::PoolKey;
use crate::entity::GaibuShowData;

pub async fn init(
    url:&str,
) -> Result<Pool<MySql>, Box<dyn std::error::Error+ Send + Sync>>{
    let pool = MySqlPoolOptions::new()
        .max_connections(20)
        .connect(&url)
        .await?;
   Ok(pool)
}

pub async fn get_gaibu_show_data_by_collection_id(
    pool:&HashMap<cmn::PoolKey, Pool<MySql>>,
    id: i64,
) -> Result<(Vec<entity::GaibuShowData>, usize), Box<dyn std::error::Error+ Send + Sync>> {
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let (_r, _c): (Vec<entity::GaibuShowData>, usize) = sqlx::query_as(
        "SELECT * FROM T_GAIBU_SHOW_DATA WHERE COLLECTION_ID = (?) LIMIT 1",
    )
        .bind(id)
        .fetch_all(_pool)
        .await
        .map(|outsource_orders| {
            let count = outsource_orders.len();
            (outsource_orders, count)
        })
        .unwrap_or_else(|err| {
            error!("Unexpected error: {:?}", err);
            (Vec::new(), 0)
        });
    Ok((_r, _c))
}

pub async fn get_gaibu_show_data_by_app_id(
    pool:&HashMap<cmn::PoolKey, Pool<MySql>>,
    app_id: &str,
) -> Result<entity::GaibuShowData, Box<dyn std::error::Error+ Send + Sync>>{
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let result :entity::GaibuShowData = sqlx::query_as(
        "SELECT * FROM T_GAIBU_SHOW_DATA WHERE APP_ID = (?) LIMIT 1",
    )
        .bind(app_id)
        .fetch_one(_pool)
        .await?;
    Ok(result)
}

pub async fn remove_gaibu_show_data_by_id(
    pool:&HashMap<cmn::PoolKey, Pool<MySql>>,
    id: &i64,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>>{

    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let _  = sqlx::query(
        "DELETE FROM T_GAIBU_SHOW_DATA WHERE ID = (?)",
    )
        .bind(id)
        .execute(_pool)
        .await?;
    Ok(())
}



pub async fn get_max_action_by_app_id_and_ext_id(
    pool:&HashMap<cmn::PoolKey, Pool<MySql>>,
    app_id:&str,
    id: i64,
) -> Result<(entity::Action, usize), Box<dyn std::error::Error+ Send + Sync>>{
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let sql = r#"
        SELECT *
        FROM T_ACTION
        WHERE
          ID != (?)
          AND C_APP_ID = (?)
        ORDER BY ID DESC
        LIMIT 1
    "#;
    let (_v, _count): (entity::Action, usize) = sqlx::query_as(
        sql,
    )
    .bind(id)
    .bind(app_id)
    .fetch_one(_pool)
    .await
    .map(|_v| (_v, 1))
        .unwrap_or_else(|err| {
            error!("Unexpected error: {:?}", err);
            (entity::Action::default(), 0)
        });
    Ok((_v, _count))
}

pub async fn get_max_collection_action_by_app_id(
    pool:&HashMap<cmn::PoolKey, Pool<MySql>>,
    app_id:&str,
) -> Result<(entity::Action, usize), Box<dyn std::error::Error+ Send + Sync>>{
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let sql = r#"
        SELECT *
        FROM T_ACTION
        WHERE
          C_APP_ID = (?)
        ORDER BY ID DESC
        LIMIT 1
    "#;
    let (_v, _count): (entity::Action, usize) = sqlx::query_as(
        sql,
    )
        .bind(app_id)
        .fetch_one(_pool)
        .await
        .map(|_v| (_v, 1))
        .unwrap_or_else(|err| {
            error!("Unexpected error: {:?}", err);
            (entity::Action::default(), 0)
        });
    Ok((_v, _count))
}


pub(crate) async fn insert_gaibu_show_data_by_entity(
    pool: &HashMap<PoolKey, Pool<MySql>>,
    gaibu_show_data: &GaibuShowData,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>>{
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let sql = r#"
    INSERT INTO
      T_GAIBU_SHOW_DATA (
        COLLECTION_ID,
        APP_ID,
        PRODUCT_CODE,
        OPERATOR_ID,
        ACTION_CODE,
        ACTION_TIME,
        ACTION_MEMO,
        CREATE_TIME,
        UPDATE_TIME
      )
    VALUES
      (
        (?),
        (?),
        (?),
        (?),
        (?),
        (?),
        (?),
        now(),
        now()
      )
    "#;
    let _ = sqlx::query(
        sql,
    )
    .bind(&gaibu_show_data.collection_id)
        .bind(&gaibu_show_data.app_id)
        .bind(&gaibu_show_data.product_code)
        .bind(&gaibu_show_data.operator_id)
        .bind(&gaibu_show_data.action_code)
        .bind(&gaibu_show_data.action_time)
        .bind(&gaibu_show_data.action_memo)
    .execute(_pool)
    .await?;
    Ok(())
}


pub(crate) async fn update_gaibu_show_data_by_entity_v2(
    pool: &HashMap<PoolKey, Pool<MySql>>,
    gaibu_show_data: &GaibuShowData,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>>{
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let sql = r#"
    UPDATE
      T_GAIBU_SHOW_DATA
        SET ACTION_CODE = (?),
        ACTION_TIME = (?),
        ACTION_MEMO = (?)
      WHERE APP_ID = (?)
    "#;
    let _ = sqlx::query(
        sql,
    )
        .bind(&gaibu_show_data.action_code)
        .bind(&gaibu_show_data.action_time)
        .bind(&gaibu_show_data.action_memo)
        .bind(&gaibu_show_data.app_id)
        .execute(_pool)
        .await?;
    Ok(())
}

pub(crate) async fn delete_gaibu_show_data_by_collection_id(
    pool:&HashMap<cmn::PoolKey, Pool<MySql>>,
    id: i64,
) -> Result<(), Box<dyn std::error::Error+ Send + Sync>>{
    let _pool = pool.get(&cmn::PoolKey::NakatoroPool).unwrap();
    let _  = sqlx::query(
    "DELETE FROM T_GAIBU_SHOW_DATA WHERE COLLECTION_ID = (?)",
    )
    .bind(id)
    .execute(_pool)
    .await?;
    Ok(())
}

