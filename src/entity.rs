use std::collections::HashMap;

use log::warn;
use sqlx::{MySql, Pool};
use sqlx::types::chrono::{NaiveDate, NaiveDateTime};

use crate::{cmn, db, entity};

#[derive(Debug, PartialEq, Clone, Default, sqlx::FromRow)]
pub struct GaibuShowData {
    #[sqlx(rename = "ID")]
    pub id: Option<i64>,
    #[sqlx(rename = "COLLECTION_ID")]
    pub collection_id: Option<i64>,
    #[sqlx(rename = "APP_ID")]
    pub app_id: Option<String>,
    #[sqlx(rename = "PRODUCT_CODE")]
    pub product_code: Option<i64>,
    #[sqlx(rename = "OPERATOR_ID")]
    pub operator_id: Option<i64>,
    #[sqlx(rename = "ACTION_CODE")]
    pub action_code: Option<String>,
    #[sqlx(rename = "ACTION_TIME")]
    pub action_time: Option<NaiveDateTime>,
    #[sqlx(rename = "ACTION_MEMO")]
    pub action_memo: Option<String>,
    #[sqlx(rename = "ACTION_PROMISE_DATE")]
    pub action_promise_date: Option<NaiveDate>,
    #[sqlx(rename = "CREATE_TIME")]
    pub create_time: Option<NaiveDateTime>,
    #[sqlx(rename = "UPDATE_TIME")]
    pub update_time: Option<NaiveDateTime>,
}


impl GaibuShowData {

    pub async fn do_flush_action_data_by_app_id(
        &mut self,
        db_pool: &HashMap<cmn::PoolKey, Pool<MySql>>,
        collection_action: &entity::Action,
        bin_log_type: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let func = "do_flush_action_data_by_app_id";
        let app_id = "";
        if let Some(app_id) = &self.app_id {
            if cmn::str_is_null(&app_id){
                warn!("{}||null app id.",&func);
                return Ok(());
            }
        }
        if bin_log_type == cmn::BIN_LOG_TYPE_DEL {
            let ext_id = collection_action.id.unwrap_or(-1_i64);
            match db::get_max_action_by_app_id_and_ext_id(db_pool, &app_id, ext_id).await {
                Ok((collection_action, cnt)) => {
                    if cnt <= 0 {
                        self.action_code = None;
                        self.action_time = None;
                        self.action_promise_date = None;
                        self.action_memo = None;
                    } else {
                        self.action_code = collection_action.action_code;
                        self.action_time = collection_action.action_time;
                        self.action_promise_date = cmn::naive_date_time_to_naive_date(&(collection_action.promise_time));
                        self.action_memo = collection_action.memo;
                    }
                }
                Err(_e) => {
                    warn!("{}||collection action error.",&func);
                }
            }
        } else {
            match db::get_max_collection_action_by_app_id(db_pool, &app_id).await {
                Ok((collection_action, cnt)) => {
                    if cnt > 0 {
                        self.action_code = collection_action.action_code;
                        self.action_time = collection_action.action_time;
                        self.action_promise_date = cmn::naive_date_time_to_naive_date(&(collection_action.promise_time));
                        self.action_memo = collection_action.memo;
                    }
                }
                Err(_e) => {
                    warn!("do_flush_action_data_by_app_id||collection action error.");
                }
            }
        }
        Ok(())
    }

}


#[derive(Debug, PartialEq, Clone, Default, sqlx::FromRow)]
pub struct Action {
    #[sqlx(rename = "ID")]
    pub id: Option<i64>,
    #[sqlx(rename = "C_APP_ID")]
    pub c_app_id: Option<String>,
    #[sqlx(rename = "ACTION_TIME")]
    pub action_time: Option<NaiveDateTime>,
    #[sqlx(rename = "C_MOBILE")]
    pub c_mobile: Option<String>,
    #[sqlx(rename = "ACTION_CODE")]
    pub action_code: Option<String>,
    #[sqlx(rename = "PROMISE_TIME")]
    pub promise_time: Option<NaiveDateTime>,
    #[sqlx(rename = "OPERATOR_ID")]
    pub operator_id: Option<i64>,
    #[sqlx(rename = "MEMO")]
    pub memo: Option<String>,
    #[sqlx(rename = "CREATE_USER")]
    pub create_user: Option<String>,
    #[sqlx(rename = "CREATE_TIME")]
    pub create_time: Option<NaiveDateTime>,
    #[sqlx(rename = "UPDATE_USER")]
    pub update_user: Option<String>,
    #[sqlx(rename = "UPDATE_TIME")]
    pub update_time: Option<NaiveDateTime>,
}
