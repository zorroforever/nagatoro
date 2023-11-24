use std::error::Error;

use num_traits::cast::FromPrimitive;
use num_traits::ToPrimitive;
use sqlx::types::BigDecimal;
use sqlx::types::chrono::{NaiveDate, NaiveDateTime};


pub static OP_TYPE_OUT: &str = "OUT";
pub static FMT_YYYYMM: &str = "%Y-%m-%d";

pub static BIN_LOG_TYPE_INS: &str = "insert";
pub static BIN_LOG_TYPE_UPD: &str = "update";
pub static BIN_LOG_TYPE_DEL: &str = "delete";

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum PoolKey {
    NakatoroPool,
    TukinashiPool,
}
#[derive(Debug)]
pub struct InitDBError {
    pub(crate) message: String,
}
impl std::fmt::Display for InitDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for InitDBError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}


pub fn f64_to_bigdecimal(
    p: &Option<f64>,
) -> Option<BigDecimal> {
    if let Some(_v) = p {
        BigDecimal::from_f64(*_v)
    } else {
        None
    }
}

pub fn i64_to_naive_date_time(
    p: &Option<i64>,
) -> Option<NaiveDateTime> {
    if let Some(_v) = p {
        NaiveDateTime::from_timestamp_micros(*_v)
    } else {
        None
    }
}

pub fn i64_to_naive_date(
    p: &Option<i64>,
) -> Option<NaiveDate> {
    if let Some(_v) = p {
        if let Some(_v) = NaiveDateTime::from_timestamp_micros(*_v) {
            Some(_v.date())
        } else {
            None
        }
    } else {
        None
    }
}

pub fn naive_date_time_to_naive_date(
    p: &Option<NaiveDateTime>,
) -> Option<NaiveDate> {
    if let Some(_v) = p {
        Some(_v.date())
    } else {
        None
    }
}

pub fn i64_to_i32(
    p: &Option<i64>,
) -> Option<i32> {
    if let Some(_v) = p {
         _v.to_i32()
    } else {
        None
    }
}
pub fn trim_start_zero_bytes(input: &[u8; 8]) -> &[u8] {
    let mut start_index = 0;

    for (index, &byte) in input.iter().enumerate() {
        if byte != 0 {
            break;
        }
        start_index = index + 1;
    }

    &input[start_index..]
}
pub fn get_str_from_json(
    obj: &serde_json::Value,
    key:&str,
) -> Option<String> {
    Some(obj[key].as_str().unwrap().to_string())
}
pub fn str_is_null(
    str:&str,
) -> bool {
    return str == "" || str.eq_ignore_ascii_case("null");
}