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

use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use tokio::sync::oneshot;

use super::errors::DatabaseError;

/// Database
pub type DBX = Option<String>;
/// Field
pub type F = Vec<u8>;
/// Field or Error
pub type FR = Result<F, DatabaseError>;
/// Success or Error
pub type R = Result<(), DatabaseError>;
/// Key and Value
pub type KV = (F, F);
/// multi KV
pub type KVS = HashMap<F, F>;
/// multi KV or Error
pub type KVSR = Result<KVS, DatabaseError>;
/// multi Fields
pub type FS = HashSet<F>;

pub type KVExp = Mutex<Box<dyn FnMut(KV) -> bool + Send>>;

pub fn kvexp(
    x: impl FnMut(KV) -> bool + Send + 'static,
) -> Mutex<Box<dyn FnMut(KV) -> bool + Send>> {
    Mutex::new(Box::new(x))
}

pub enum EngineCmd {
    Read((DBX, F), oneshot::Sender<FR>),
    Write((DBX, KV), oneshot::Sender<R>),
    Delete((DBX, F), oneshot::Sender<R>),
    Apply((DBX, KVS), oneshot::Sender<R>),
    DeleteMany((DBX, FS), oneshot::Sender<R>),
    DeleteIf((DBX, KVExp), oneshot::Sender<R>),
    Search((DBX, KVExp), oneshot::Sender<KVSR>),
    Stop,
}
