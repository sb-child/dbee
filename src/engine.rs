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
    ops::ControlFlow,
    sync::Mutex,
};

use super::{
    commands::{EngineCmd, KVExp, DBX, F, FR, FS, KVS, KVSR, R},
    errors::DatabaseError,
    options::{EngineOptions, Locked},
};
use speedb::{self, WriteBatchWithTransaction, DB};
use tokio::sync::{
    mpsc::{self, error::SendTimeoutError},
    oneshot,
};
use tokio::task::JoinHandle;

pub struct Engine {
    // db_opts: speedb::Options,
    db_handle: JoinHandle<()>,
    eng_tx: mpsc::Sender<EngineCmd>,
    default_chan_timeout: tokio::time::Duration,
}

impl Engine {
    pub fn new(opt: EngineOptions<Locked>) -> Result<Self, DatabaseError> {
        let mut db_opts = speedb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        let db = speedb::DB::open_cf(&db_opts, opt.db_path, opt.databases);
        let db = match db {
            Ok(x) => x,
            Err(e) => return Err(DatabaseError::DatabaseError(e)),
        };
        let (eng_tx, mut eng_rx) = mpsc::channel::<EngineCmd>(opt.channel_size);
        let h = tokio::task::spawn_blocking(move || loop {
            let r: Option<EngineCmd> = eng_rx.blocking_recv();
            let r = if let Some(x) = r {
                x
            } else {
                return;
            };
            match r {
                EngineCmd::Read((dbx, k), ret) => {
                    if let ControlFlow::Break(_) = db_op_read(dbx, &db, ret, k) {
                        continue;
                    }
                }
                EngineCmd::Write((dbx, (k, v)), ret) => {
                    if let ControlFlow::Break(_) = db_op_write(dbx, &db, ret, k, v) {
                        continue;
                    }
                }
                EngineCmd::Delete((dbx, k), ret) => {
                    if let ControlFlow::Break(_) = db_op_delete(dbx, &db, ret, k) {
                        continue;
                    }
                }
                EngineCmd::Apply((dbx, hm), ret) => {
                    if let ControlFlow::Break(_) = db_op_apply(dbx, &db, ret, hm) {
                        continue;
                    }
                }
                EngineCmd::DeleteMany((dbx, hs), ret) => {
                    if let ControlFlow::Break(_) = db_op_delete_many(dbx, &db, ret, hs) {
                        continue;
                    }
                }
                EngineCmd::DeleteIf((dbx, f), ret) => {
                    if let ControlFlow::Break(_) = db_op_delete_if(dbx, &db, ret, f) {
                        continue;
                    }
                }
                EngineCmd::Search((dbx, f), ret) => {
                    if let ControlFlow::Break(_) = db_op_search(dbx, &db, ret, f) {
                        continue;
                    }
                }
                EngineCmd::Stop => {
                    break;
                }
            };
        });
        Ok(Self {
            db_handle: h,
            eng_tx,
            default_chan_timeout: opt.channel_timeout,
        })
    }

    pub async fn exec(
        &self,
        cmd: EngineCmd,
        timeout: Option<tokio::time::Duration>,
    ) -> Result<(), SendTimeoutError<EngineCmd>> {
        self.eng_tx
            .send_timeout(cmd, timeout.unwrap_or(self.default_chan_timeout))
            .await
    }
}

fn db_op_read(dbx: DBX, db: &DB, ret: oneshot::Sender<FR>, k: F) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let res = db.get_cf(&dbx, k);
        ret.send(match res {
            Ok(x) => {
                if let Some(x) = x {
                    Ok(x.into())
                } else {
                    Err(DatabaseError::InvalidKey())
                }
            }
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let res = db.get(k);
        ret.send(match res {
            Ok(x) => {
                if let Some(x) = x {
                    Ok(x.into())
                } else {
                    Err(DatabaseError::InvalidKey())
                }
            }
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

fn db_op_write(dbx: DBX, db: &DB, ret: oneshot::Sender<R>, k: F, v: F) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let res = db.put_cf(&dbx, k, v);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let res = db.put(k, v);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

fn db_op_delete(dbx: DBX, db: &DB, ret: oneshot::Sender<R>, k: F) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let res = db.delete_cf(&dbx, k);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let res = db.delete(k);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

fn db_op_apply(dbx: DBX, db: &DB, ret: oneshot::Sender<R>, hm: KVS) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let mut t = WriteBatchWithTransaction::default();
        for (k, v) in hm {
            t.put_cf(&dbx, k, v);
        }
        let res = db.write(t);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let mut t = WriteBatchWithTransaction::default();
        for (k, v) in hm {
            t.put(k, v);
        }
        let res = db.write(t);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

fn db_op_delete_many(dbx: DBX, db: &DB, ret: oneshot::Sender<R>, hs: FS) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let mut t = WriteBatchWithTransaction::default();
        for k in hs {
            t.delete_cf(&dbx, k);
        }
        let res = db.write(t);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let mut t = WriteBatchWithTransaction::default();
        for k in hs {
            t.delete(k);
        }
        let res = db.write(t);
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

fn db_op_delete_if(dbx: DBX, db: &DB, ret: oneshot::Sender<R>, f: KVExp) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let res = db
            .iterator_cf(&dbx, speedb::IteratorMode::Start)
            .filter(|x| {
                let func = f.lock();
                let mut func = if let Ok(func) = func {
                    func
                } else {
                    return false;
                };
                let (k, v) = if let Ok(x) = x {
                    x
                } else {
                    return false;
                };
                func((k.to_vec(), v.to_vec()))
            })
            .try_for_each(|x| {
                let (k, _) = x.unwrap(); // maybe safe
                db.delete_cf(&dbx, k)
            });
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let res = db
            .iterator(speedb::IteratorMode::Start)
            .filter(|x| {
                let func = f.lock();
                let mut func = if let Ok(func) = func {
                    func
                } else {
                    return false;
                };
                let (k, v) = if let Ok(x) = x {
                    x
                } else {
                    return false;
                };
                func((k.to_vec(), v.to_vec()))
            })
            .try_for_each(|x| {
                let (k, _) = x.unwrap(); // should be safe
                db.delete(k)
            });
        ret.send(match res {
            Ok(_) => Ok(()),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

fn db_op_search(dbx: DBX, db: &DB, ret: oneshot::Sender<KVSR>, f: KVExp) -> ControlFlow<()> {
    if let Some(dbx) = dbx {
        let dbx = db.cf_handle(&dbx);
        let dbx = match dbx {
            Some(dbx) => dbx,
            None => {
                ret.send(Err(DatabaseError::InvalidDatabase())).unwrap();
                return ControlFlow::Break(());
            }
        };
        let mut hm: KVS = HashMap::new();
        let res = db
            .iterator_cf(&dbx, speedb::IteratorMode::Start)
            .filter(|x| {
                let func = f.lock();
                let mut func = if let Ok(func) = func {
                    func
                } else {
                    return false;
                };
                let (k, v) = if let Ok(x) = x {
                    x
                } else {
                    // process error later
                    return true;
                };
                func((k.to_vec(), v.to_vec()))
            })
            .try_for_each(|x| {
                let (k, v) = match x {
                    Ok(x) => x,
                    Err(e) => return Err(e),
                };
                hm.insert(k.to_vec(), v.to_vec());
                Ok(())
            });
        ret.send(match res {
            Ok(_) => Ok(hm),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    } else {
        let mut hm: KVS = HashMap::new();
        let res = db
            .iterator(speedb::IteratorMode::Start)
            .filter(|x| {
                let func = f.lock();
                let mut func = if let Ok(func) = func {
                    func
                } else {
                    return false;
                };
                let (k, v) = if let Ok(x) = x {
                    x
                } else {
                    // process error later
                    return true;
                };
                func((k.to_vec(), v.to_vec()))
            })
            .try_for_each(|x| {
                let (k, v) = match x {
                    Ok(x) => x,
                    Err(e) => return Err(e),
                };
                hm.insert(k.to_vec(), v.to_vec());
                Ok(())
            });
        ret.send(match res {
            Ok(_) => Ok(hm),
            Err(e) => Err(DatabaseError::DatabaseError(e)),
        })
        .unwrap();
    }
    ControlFlow::Continue(())
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.db_handle.abort();
    }
}

#[tokio::test]
async fn test_db_exec() {
    let opts = EngineOptions::new()
        .with_database_name(["test-1", "test-2"])
        .with_path("_db_test")
        .lock();
    let db = Engine::new(opts).unwrap();

    for dbs in [None, Some("test-1".to_owned()), Some("test-2".to_owned())] {
        // Write
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(
            EngineCmd::Write((dbs.clone(), (b"aa".to_vec(), b"bb".to_vec())), tx),
            None,
        )
        .await
        .unwrap();
        rx.await.unwrap().unwrap();

        // Read
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(EngineCmd::Read((dbs.clone(), (b"aa".to_vec())), tx), None)
            .await
            .unwrap();
        let res = rx.await.unwrap().unwrap();
        assert!(res == b"bb".to_vec());

        // Delete
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(EngineCmd::Delete((dbs.clone(), (b"aa".to_vec())), tx), None)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // Delete twice (should work)
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(EngineCmd::Delete((dbs.clone(), (b"aa".to_vec())), tx), None)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // Read again
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(EngineCmd::Read((dbs.clone(), (b"aa".to_vec())), tx), None)
            .await
            .unwrap();
        let res = rx.await.unwrap();
        assert!(res == Err(DatabaseError::InvalidKey()));

        // Apply
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut hm: KVS = HashMap::new();
        hm.insert(b"hello".into(), b"world".into());
        hm.insert(b"goodbye".into(), b"WORLD".into());
        hm.insert(b"_goodbye".into(), b"__WORLD".into());
        hm.insert(b"nothing".into(), b"nya".into());
        db.exec(EngineCmd::Apply((dbs.clone(), (hm)), tx), None)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // Search
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(
            EngineCmd::Search(
                (
                    dbs.clone(),
                    (Mutex::new(Box::new(|(k, v)| {
                        k.starts_with(b"_") || v.starts_with(b"n")
                    }))),
                ),
                tx,
            ),
            None,
        )
        .await
        .unwrap();
        let res = rx.await.unwrap().unwrap();
        let mut hm: KVS = HashMap::new();
        hm.insert(b"_goodbye".into(), b"__WORLD".into());
        hm.insert(b"nothing".into(), b"nya".into());
        assert!(res == hm);

        // DeleteMany
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut hs: FS = HashSet::new();
        hs.insert(b"hello".into());
        hs.insert(b"goodbye".into());
        hs.insert(b"not-exist".into()); // (should work)
        db.exec(EngineCmd::DeleteMany((dbs.clone(), (hs)), tx), None)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        // Read again
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(
            EngineCmd::Read((dbs.clone(), (b"hello".to_vec())), tx),
            None,
        )
        .await
        .unwrap();
        let res = rx.await.unwrap();
        assert!(res == Err(DatabaseError::InvalidKey()));

        // Search again
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(
            EngineCmd::Search(
                (
                    dbs.clone(),
                    (Mutex::new(Box::new(|(k, _v)| k.starts_with(b"not-")))),
                ),
                tx,
            ),
            None,
        )
        .await
        .unwrap();
        let res = rx.await.unwrap().unwrap();
        let hm: KVS = HashMap::new();
        assert!(res == hm);

        // DeleteIf
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(
            EngineCmd::DeleteIf(
                (
                    dbs.clone(),
                    (Mutex::new(Box::new(|(k, v)| {
                        k.starts_with(b"_") || v.starts_with(b"n")
                    }))),
                ),
                tx,
            ),
            None,
        )
        .await
        .unwrap();
        rx.await.unwrap().unwrap();

        // Search again
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.exec(
            EngineCmd::Search(
                (
                    dbs.clone(),
                    (Mutex::new(Box::new(|(k, v)| {
                        k.starts_with(b"_") || v.starts_with(b"n")
                    }))),
                ),
                tx,
            ),
            None,
        )
        .await
        .unwrap();
        let res = rx.await.unwrap().unwrap();
        let hm: KVS = HashMap::new();
        assert!(res == hm);
    }

    return;
}
