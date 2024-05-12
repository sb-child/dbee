// Copyright 2024 sb-child
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::{DateTime, TimeZone, Timelike, Utc};
use std::convert::TryInto;

fn byte_to_bytes(val: u8) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn short_to_bytes(val: i16) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn int_to_bytes(val: i32) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn long_to_bytes(val: i64) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn ushort_to_bytes(val: i16) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn uint_to_bytes(val: i32) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn ulong_to_bytes(val: i64) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn float_to_bytes(val: f32) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn double_to_bytes(val: f64) -> Vec<u8> {
    val.to_le_bytes().to_vec()
}

fn bool_to_bytes(val: bool) -> Vec<u8> {
    if val { 1_u8 } else { 0 }.to_le_bytes().to_vec()
}

fn datetime_to_bytes(val: chrono::DateTime<chrono::Utc>) -> Vec<u8> {
    [
        val.timestamp().to_le_bytes(),
        (val.nanosecond() as u64).to_le_bytes(),
    ]
    .concat()
    .to_vec()
}

fn string_to_bytes(val: String) -> Vec<u8> {
    val.as_bytes().to_vec()
}

fn bytes_to_bytes(val: &[u8]) -> Vec<u8> {
    val.to_vec()
}

fn bytes_to_byte(bytes: &[u8]) -> Option<u8> {
    if bytes.len() != 1 {
        return None;
    }
    Some(bytes[0])
}

fn bytes_to_short(bytes: &[u8]) -> Option<i16> {
    if bytes.len() != 2 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(i16::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_int(bytes: &[u8]) -> Option<i32> {
    if bytes.len() != 4 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(i32::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_long(bytes: &[u8]) -> Option<i64> {
    if bytes.len() != 8 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(i64::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_ushort(bytes: &[u8]) -> Option<u16> {
    if bytes.len() != 2 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(u16::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_uint(bytes: &[u8]) -> Option<u32> {
    if bytes.len() != 4 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(u32::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_ulong(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(u64::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_float(bytes: &[u8]) -> Option<f32> {
    if bytes.len() != 4 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(f32::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_double(bytes: &[u8]) -> Option<f64> {
    if bytes.len() != 8 {
        return None;
    }
    let x = bytes.try_into();
    if let Ok(x) = x {
        Some(f64::from_le_bytes(x))
    } else {
        None
    }
}

fn bytes_to_bool(bytes: &[u8]) -> Option<bool> {
    if bytes.len() != 1 {
        return None;
    }
    Some(bytes[0] != 0)
}

fn bytes_to_datetime(bytes: &[u8]) -> Option<DateTime<Utc>> {
    let timestamp_bytes: [u8; 8] = if let Ok(x) = bytes[0..8].try_into() {
        x
    } else {
        return None;
    };
    let nanosecond_bytes: [u8; 8] = if let Ok(x) = bytes[8..16].try_into() {
        x
    } else {
        return None;
    };
    let timestamp = i64::from_le_bytes(timestamp_bytes);
    let nanosecond = u64::from_le_bytes(nanosecond_bytes) as u32;
    let datetime = Utc.timestamp_opt(timestamp, nanosecond);
    match datetime {
        chrono::offset::LocalResult::Single(x) => Some(x),
        chrono::offset::LocalResult::Ambiguous(_, _) => None,
        chrono::offset::LocalResult::None => None,
    }
}

fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

pub enum Type {
    Byte(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Logic(bool),
    DateTime(DateTime<Utc>),
    Text(String),
    Bytes(Vec<u8>),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
}
