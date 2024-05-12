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

use std::{marker::PhantomData, path::Path};

pub struct Editing;

pub struct Locked;

pub struct EngineOptions<State = Editing> {
    pub(crate) databases: Vec<Box<str>>,
    pub(crate) db_path: Box<Path>,
    pub(crate) channel_size: usize,
    pub(crate) channel_timeout: tokio::time::Duration,
    state: PhantomData<State>,
}

impl EngineOptions {
    pub fn new() -> Self {
        Self {
            databases: vec![],
            db_path: Path::new("").into(),
            state: PhantomData,
            channel_size: 1,
            channel_timeout: tokio::time::Duration::from_secs(1),
        }
    }
}

impl EngineOptions<Editing> {
    pub fn with_path<P>(mut self, x: P) -> Self
    where
        P: AsRef<Path>,
    {
        self.db_path = x.as_ref().to_path_buf().into_boxed_path();
        self
    }
    pub fn with_database_name<I, N>(mut self, n: I) -> Self
    where
        I: IntoIterator<Item = N>,
        N: AsRef<str>,
    {
        self.databases.extend(
            n.into_iter()
                .map(|x| x.as_ref().to_string().into_boxed_str()),
        );
        self
    }
    pub fn with_channel_size(mut self, n: usize) -> Self {
        self.channel_size = n;
        self
    }
    pub fn with_channel_timeout(mut self, n: tokio::time::Duration) -> Self {
        self.channel_timeout = n;
        self
    }
    pub fn lock(self) -> EngineOptions<Locked> {
        EngineOptions {
            databases: self.databases,
            db_path: self.db_path,
            channel_size: self.channel_size,
            channel_timeout: self.channel_timeout,
            state: PhantomData,
        }
    }
}

impl EngineOptions<Locked> {
    pub fn unlock(self) -> EngineOptions<Editing> {
        EngineOptions {
            databases: self.databases,
            db_path: self.db_path,
            channel_size: self.channel_size,
            channel_timeout: self.channel_timeout,
            state: PhantomData,
        }
    }
}
