use futures::StreamExt;
use redis::AsyncCommands;
use std::collections::HashSet;
use tracing::debug;

use crate::TestResult;

pub async fn run_tests(
    host: &str,
    port: u16,
) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
    let client = redis::Client::open(format!("redis://{}:{}", host, port))?;
    let mut con = client.get_async_connection().await?;
    let mut results = Vec::new();

    // Test SET command
    let set_result = test_set(&mut con).await;
    results.push(set_result);

    // Test GET command
    let get_result = test_get(&mut con).await;
    results.push(get_result);

    // Test DEL command
    let del_result = test_del(&mut con).await;
    results.push(del_result);

    // Test EXISTS command
    let exists_result = test_exists(&mut con).await;
    results.push(exists_result);

    // Test APPEND command
    let append_result = test_append(&mut con).await;
    results.push(append_result);

    // Test STRLEN command
    let strlen_result = test_strlen(&mut con).await;
    results.push(strlen_result);

    // Hash command tests
    let hset_hget_result = test_hset_hget(&mut con).await;
    results.push(hset_hget_result);
    let hgetall_result = test_hgetall(&mut con).await;
    results.push(hgetall_result);
    let hexists_result = test_hexists(&mut con).await;
    results.push(hexists_result);
    let hdel_result = test_hdel(&mut con).await;
    results.push(hdel_result);
    let hlen_result = test_hlen(&mut con).await;
    results.push(hlen_result);
    let hkeys_result = test_hkeys(&mut con).await;
    results.push(hkeys_result);
    let hvals_result = test_hvals(&mut con).await;
    results.push(hvals_result);

    // List command tests
    let lpush_rpush_llen_result = test_lpush_rpush_llen(&mut con).await;
    results.push(lpush_rpush_llen_result);
    let lrange_result = test_lrange(&mut con).await;
    results.push(lrange_result);
    let lpop_result = test_lpop(&mut con).await;
    results.push(lpop_result);
    let rpop_result = test_rpop(&mut con).await;
    results.push(rpop_result);

    // Set command tests
    let sadd_scard_result = test_sadd_scard(&mut con).await;
    results.push(sadd_scard_result);
    let smembers_result = test_smembers(&mut con).await;
    results.push(smembers_result);
    let sismember_result = test_sismember(&mut con).await;
    results.push(sismember_result);
    let srem_result = test_srem(&mut con).await;
    results.push(srem_result);

    // Key command tests
    let keys_result = test_keys(&mut con).await;
    results.push(keys_result);
    let ttl_expire_result = test_ttl_expire(&mut con).await;
    results.push(ttl_expire_result);
    let rename_result = test_rename(&mut con).await;
    results.push(rename_result);
    let type_result = test_type(&mut con).await;
    results.push(type_result);

    // Server command tests
    let ping_result = test_ping(&mut con).await;
    results.push(ping_result);
    let echo_result = test_echo(&mut con).await;
    results.push(echo_result);
    let info_result = test_info(&mut con).await;
    results.push(info_result);
    let save_result = test_save(&mut con).await;
    results.push(save_result);

    // New string command tests
    results.push(test_incr_decr(&mut con).await);
    results.push(test_incrby_decrby(&mut con).await);
    results.push(test_incrbyfloat(&mut con).await);
    results.push(test_mget_mset(&mut con).await);
    results.push(test_msetnx(&mut con).await);
    results.push(test_setnx(&mut con).await);
    results.push(test_setex(&mut con).await);
    results.push(test_getrange(&mut con).await);
    results.push(test_setrange(&mut con).await);
    results.push(test_getdel(&mut con).await);

    // New key command tests
    results.push(test_persist(&mut con).await);
    results.push(test_pttl_pexpire(&mut con).await);
    results.push(test_unlink(&mut con).await);
    results.push(test_renamenx(&mut con).await);
    results.push(test_copy(&mut con).await);
    results.push(test_touch(&mut con).await);
    results.push(test_scan(&mut con).await);
    results.push(test_dbsize(&mut con).await);
    results.push(test_time(&mut con).await);

    // New list command tests
    results.push(test_lindex(&mut con).await);
    results.push(test_lset(&mut con).await);
    results.push(test_ltrim(&mut con).await);
    results.push(test_linsert(&mut con).await);
    results.push(test_lrem(&mut con).await);
    results.push(test_lpushx_rpushx(&mut con).await);

    // New set command tests
    results.push(test_spop(&mut con).await);
    results.push(test_smove(&mut con).await);
    results.push(test_sinter(&mut con).await);
    results.push(test_sunion(&mut con).await);
    results.push(test_sdiff(&mut con).await);
    results.push(test_sinterstore(&mut con).await);

    // New hash command tests
    results.push(test_hmget(&mut con).await);
    results.push(test_hincrby(&mut con).await);
    results.push(test_hsetnx(&mut con).await);
    results.push(test_hstrlen(&mut con).await);

    // Sorted set command tests
    results.push(test_zadd_zscore(&mut con).await);
    results.push(test_zcard(&mut con).await);
    results.push(test_zrank(&mut con).await);
    results.push(test_zrange(&mut con).await);
    results.push(test_zrem(&mut con).await);
    results.push(test_zincrby(&mut con).await);
    results.push(test_zcount(&mut con).await);
    results.push(test_zpopmin_zpopmax(&mut con).await);

    // Bitmap command tests
    results.push(test_setbit_getbit(&mut con).await);
    results.push(test_bitcount(&mut con).await);

    // HyperLogLog command tests
    results.push(test_pfadd_pfcount(&mut con).await);
    results.push(test_pfmerge(&mut con).await);

    // Transaction command tests
    results.push(test_multi_exec(&mut con).await);
    results.push(test_multi_discard(&mut con).await);

    // Server command tests
    results.push(test_config(&mut con).await);
    results.push(test_command_cmd(&mut con).await);
    results.push(test_select(&mut con).await);
    results.push(test_flushdb(&mut con).await);

    // PubSub test
    let pubsub_result = test_pubsub(host, port).await;
    results.push(pubsub_result);

    // Concurrent connections test
    let concurrent_result = test_concurrent_connections(host, port).await;
    results.push(concurrent_result);

    Ok(results)
}

// Helper macro for creating test results
macro_rules! ok {
    ($name:expr) => {
        TestResult {
            name: $name.to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        }
    };
}

macro_rules! fail {
    ($name:expr, $msg:expr) => {
        TestResult {
            name: $name.to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some($msg.to_string()),
        }
    };
}

async fn test_set(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running SET command test");
    match con.set::<_, _, ()>("test_key", "test_value").await {
        Ok(_) => TestResult {
            name: "SET Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Err(e) => TestResult {
            name: "SET Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Failed to set test key: {}", e)),
        },
    }
}

async fn test_get(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running GET command test");
    match con.get::<_, String>("test_key").await {
        Ok(value) => {
            if value == "test_value" {
                TestResult {
                    name: "GET Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "GET Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Expected 'test_value', got '{}'", value)),
                }
            }
        }
        Err(e) => TestResult {
            name: "GET Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_del(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running DEL command test");
    // First set a key
    if let Err(e) = con.set::<_, _, ()>("test_del_key", "value").await {
        return TestResult {
            name: "DEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Failed to set test key: {}", e)),
        };
    }

    match con.del::<_, i32>("test_del_key").await {
        Ok(1) => TestResult {
            name: "DEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(n) => TestResult {
            name: "DEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 1 key deleted, got {}", n)),
        },
        Err(e) => TestResult {
            name: "DEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_exists(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running EXISTS command test");
    // First set a key
    if let Err(e) = con.set::<_, _, ()>("test_exists_key", "value").await {
        return TestResult {
            name: "EXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Failed to set test key: {}", e)),
        };
    }

    match con.exists::<_, i32>("test_exists_key").await {
        Ok(1) => TestResult {
            name: "EXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(n) => TestResult {
            name: "EXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 1, got {}", n)),
        },
        Err(e) => TestResult {
            name: "EXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_append(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running APPEND command test");
    // Ensure a fresh key
    if let Err(e) = con.del::<_, i32>("test_append_key").await {
        let _ = e;
    }
    if let Err(e) = con.set::<_, _, ()>("test_append_key", "hello").await {
        return TestResult {
            name: "APPEND Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Failed to set base key: {}", e)),
        };
    }

    match redis::cmd("APPEND")
        .arg("test_append_key")
        .arg(" world")
        .query_async::<_, i64>(con)
        .await
    {
        Ok(len) => {
            // Expect new length to be 11
            if len == 11 {
                match con.get::<_, String>("test_append_key").await {
                    Ok(val) if val == "hello world" => TestResult {
                        name: "APPEND Command".to_string(),
                        protocol: "Redis".to_string(),
                        success: true,
                        message: None,
                    },
                    Ok(val) => TestResult {
                        name: "APPEND Command".to_string(),
                        protocol: "Redis".to_string(),
                        success: false,
                        message: Some(format!("Expected 'hello world', got '{}'", val)),
                    },
                    Err(e) => TestResult {
                        name: "APPEND Command".to_string(),
                        protocol: "Redis".to_string(),
                        success: false,
                        message: Some(e.to_string()),
                    },
                }
            } else {
                TestResult {
                    name: "APPEND Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Expected length 11, got {}", len)),
                }
            }
        }
        Err(e) => TestResult {
            name: "APPEND Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_strlen(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running STRLEN command test");
    if let Err(e) = con.set::<_, _, ()>("test_strlen_key", "len12_chars").await {
        return TestResult {
            name: "STRLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Failed to set key: {}", e)),
        };
    }

    match redis::cmd("STRLEN")
        .arg("test_strlen_key")
        .query_async::<_, i64>(con)
        .await
    {
        Ok(11) => TestResult {
            name: "STRLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(len) => TestResult {
            name: "STRLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 11, got {}", len)),
        },
        Err(e) => TestResult {
            name: "STRLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

// ---- Hash command tests ----
async fn test_hset_hget(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HSET/HGET command test");
    let key = "test_hash";
    let _ = con.del::<_, i32>(key).await;

    let set_res = redis::cmd("HSET")
        .arg(key)
        .arg("field1")
        .arg("v1")
        .query_async::<_, i64>(con)
        .await;
    match set_res {
        Ok(_) => match redis::cmd("HGET")
            .arg(key)
            .arg("field1")
            .query_async::<_, Option<String>>(con)
            .await
        {
            Ok(Some(v)) if v == "v1" => TestResult {
                name: "HSET/HGET Command".to_string(),
                protocol: "Redis".to_string(),
                success: true,
                message: None,
            },
            Ok(Some(v)) => TestResult {
                name: "HSET/HGET Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Expected 'v1', got '{}'", v)),
            },
            Ok(None) => TestResult {
                name: "HSET/HGET Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some("Got None for existing field".to_string()),
            },
            Err(e) => TestResult {
                name: "HSET/HGET Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(e.to_string()),
            },
        },
        Err(e) => TestResult {
            name: "HSET/HGET Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_hgetall(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HGETALL command test");
    let key = "test_hash_all";
    let _ = con.del::<_, i32>(key).await;

    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("a")
        .arg("1")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("b")
        .arg("2")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("HGETALL")
        .arg(key)
        .query_async::<_, Vec<(String, String)>>(con)
        .await
    {
        Ok(items) => {
            let map: std::collections::HashMap<_, _> = items.into_iter().collect();
            if map.get("a") == Some(&"1".to_string()) && map.get("b") == Some(&"2".to_string()) {
                TestResult {
                    name: "HGETALL Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "HGETALL Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Unexpected map: {:?}", map)),
                }
            }
        }
        Err(e) => TestResult {
            name: "HGETALL Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_hexists(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HEXISTS command test");
    let key = "test_hash_exists";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("foo")
        .arg("bar")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("HEXISTS")
        .arg(key)
        .arg("foo")
        .query_async::<_, i64>(con)
        .await
    {
        Ok(1) => TestResult {
            name: "HEXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "HEXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 1, got {}", v)),
        },
        Err(e) => TestResult {
            name: "HEXISTS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_hdel(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HDEL command test");
    let key = "test_hash_del";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("x")
        .arg("1")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("HDEL")
        .arg(key)
        .arg("x")
        .query_async::<_, i64>(con)
        .await
    {
        Ok(1) => TestResult {
            name: "HDEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "HDEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 1, got {}", v)),
        },
        Err(e) => TestResult {
            name: "HDEL Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_hlen(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HLEN command test");
    let key = "test_hash_len";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("a")
        .arg("1")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("b")
        .arg("2")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("HLEN").arg(key).query_async::<_, i64>(con).await {
        Ok(2) => TestResult {
            name: "HLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "HLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 2, got {}", v)),
        },
        Err(e) => TestResult {
            name: "HLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_hkeys(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HKEYS command test");
    let key = "test_hash_keys";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("a")
        .arg("1")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("b")
        .arg("2")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("HKEYS")
        .arg(key)
        .query_async::<_, Vec<String>>(con)
        .await
    {
        Ok(mut keys) => {
            keys.sort();
            if keys == vec!["a".to_string(), "b".to_string()] {
                TestResult {
                    name: "HKEYS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "HKEYS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Unexpected keys: {:?}", keys)),
                }
            }
        }
        Err(e) => TestResult {
            name: "HKEYS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_hvals(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running HVALS command test");
    let key = "test_hash_vals";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("a")
        .arg("1")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("b")
        .arg("2")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("HVALS")
        .arg(key)
        .query_async::<_, Vec<String>>(con)
        .await
    {
        Ok(mut vals) => {
            vals.sort();
            if vals == vec!["1".to_string(), "2".to_string()] {
                TestResult {
                    name: "HVALS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "HVALS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Unexpected vals: {:?}", vals)),
                }
            }
        }
        Err(e) => TestResult {
            name: "HVALS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

// ---- List command tests ----
async fn test_lpush_rpush_llen(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running LPUSH/RPUSH/LLEN command test");
    let key = "test_list_len";
    let _ = con.del::<_, i32>(key).await;

    let r1 = redis::cmd("LPUSH")
        .arg(key)
        .arg("b")
        .arg("a")
        .query_async::<_, i64>(con)
        .await; // list now: a, b (head to tail)
    let r2 = redis::cmd("RPUSH")
        .arg(key)
        .arg("c")
        .query_async::<_, i64>(con)
        .await; // list: a, b, c

    if r1.is_err() || r2.is_err() {
        return TestResult {
            name: "LPUSH/RPUSH/LLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("Failed to push to list".to_string()),
        };
    }

    match redis::cmd("LLEN").arg(key).query_async::<_, i64>(con).await {
        Ok(3) => TestResult {
            name: "LLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "LLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 3, got {}", v)),
        },
        Err(e) => TestResult {
            name: "LLEN Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_lrange(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running LRANGE command test");
    let key = "test_list_range";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("LRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .query_async::<_, Vec<String>>(con)
        .await
    {
        Ok(vals) => {
            if vals == vec!["a", "b", "c"] {
                TestResult {
                    name: "LRANGE Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "LRANGE Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Expected [a,b,c], got {:?}", vals)),
                }
            }
        }
        Err(e) => TestResult {
            name: "LRANGE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_lpop(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running LPOP command test");
    let key = "test_list_lpop";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("x")
        .arg("y")
        .arg("z")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("LPOP")
        .arg(key)
        .query_async::<_, Option<String>>(con)
        .await
    {
        Ok(Some(v)) if v == "x" => TestResult {
            name: "LPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(Some(v)) => TestResult {
            name: "LPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 'x', got '{}'", v)),
        },
        Ok(None) => TestResult {
            name: "LPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("Got None from non-empty list".to_string()),
        },
        Err(e) => TestResult {
            name: "LPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_rpop(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running RPOP command test");
    let key = "test_list_rpop";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("x")
        .arg("y")
        .arg("z")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("RPOP")
        .arg(key)
        .query_async::<_, Option<String>>(con)
        .await
    {
        Ok(Some(v)) if v == "z" => TestResult {
            name: "RPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(Some(v)) => TestResult {
            name: "RPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 'z', got '{}'", v)),
        },
        Ok(None) => TestResult {
            name: "RPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("Got None from non-empty list".to_string()),
        },
        Err(e) => TestResult {
            name: "RPOP Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

// ---- Set command tests ----
async fn test_sadd_scard(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running SADD/SCARD command test");
    let key = "test_set_card";
    let _ = con.del::<_, i32>(key).await;

    let add_res = redis::cmd("SADD")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("a")
        .query_async::<_, i64>(con)
        .await;
    match add_res {
        Ok(2) => match redis::cmd("SCARD")
            .arg(key)
            .query_async::<_, i64>(con)
            .await
        {
            Ok(2) => TestResult {
                name: "SADD/SCARD Command".to_string(),
                protocol: "Redis".to_string(),
                success: true,
                message: None,
            },
            Ok(v) => TestResult {
                name: "SADD/SCARD Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Expected card 2, got {}", v)),
            },
            Err(e) => TestResult {
                name: "SADD/SCARD Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(e.to_string()),
            },
        },
        Ok(v) => TestResult {
            name: "SADD/SCARD Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected added 2, got {}", v)),
        },
        Err(e) => TestResult {
            name: "SADD/SCARD Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_smembers(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running SMEMBERS command test");
    let key = "test_set_members";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("SADD")
        .arg(key)
        .arg("x")
        .arg("y")
        .arg("z")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("SMEMBERS")
        .arg(key)
        .query_async::<_, Vec<String>>(con)
        .await
    {
        Ok(vals) => {
            let set: HashSet<String> = vals.into_iter().collect();
            let expected: HashSet<String> =
                ["x", "y", "z"].into_iter().map(|s| s.to_string()).collect();
            if set == expected {
                TestResult {
                    name: "SMEMBERS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "SMEMBERS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some("Members mismatch".to_string()),
                }
            }
        }
        Err(e) => TestResult {
            name: "SMEMBERS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_sismember(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running SISMEMBER command test");
    let key = "test_set_ismember";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("SADD")
        .arg(key)
        .arg("foo")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("SISMEMBER")
        .arg(key)
        .arg("foo")
        .query_async::<_, i64>(con)
        .await
    {
        Ok(1) => TestResult {
            name: "SISMEMBER Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "SISMEMBER Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 1, got {}", v)),
        },
        Err(e) => TestResult {
            name: "SISMEMBER Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_srem(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running SREM command test");
    let key = "test_set_srem";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("SADD")
        .arg(key)
        .arg("a")
        .arg("b")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("SREM")
        .arg(key)
        .arg("a")
        .query_async::<_, i64>(con)
        .await
    {
        Ok(1) => TestResult {
            name: "SREM Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "SREM Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 1, got {}", v)),
        },
        Err(e) => TestResult {
            name: "SREM Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

// ---- Key command tests ----
async fn test_keys(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running KEYS command test");
    let k1 = "testkey:keys:a";
    let k2 = "testkey:keys:b";
    let _ = con.del::<_, i32>(k1).await;
    let _ = con.del::<_, i32>(k2).await;
    let _ = con.set::<_, _, ()>(k1, "1").await;
    let _ = con.set::<_, _, ()>(k2, "2").await;

    match redis::cmd("KEYS")
        .arg("testkey:keys:*")
        .query_async::<_, Vec<String>>(con)
        .await
    {
        Ok(mut keys) => {
            keys.sort();
            if keys == vec![k1.to_string(), k2.to_string()] {
                TestResult {
                    name: "KEYS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "KEYS Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Unexpected keys: {:?}", keys)),
                }
            }
        }
        Err(e) => TestResult {
            name: "KEYS Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_ttl_expire(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running TTL/EXPIRE command test");
    let key = "test_ttl_expire";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "v").await;

    // TTL should be -1 for no expiration
    let ttl1 = redis::cmd("TTL").arg(key).query_async::<_, i64>(con).await;
    if let Ok(v) = ttl1 {
        if v != -1 {
            return TestResult {
                name: "TTL/EXPIRE Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Expected TTL -1, got {}", v)),
            };
        }
    } else {
        return TestResult {
            name: "TTL/EXPIRE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("TTL failed".to_string()),
        };
    }

    // Set expire and check TTL is non-negative
    let exp = redis::cmd("EXPIRE")
        .arg(key)
        .arg(10)
        .query_async::<_, i64>(con)
        .await;
    if let Ok(v) = exp {
        if v != 1 {
            return TestResult {
                name: "TTL/EXPIRE Command".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Expected EXPIRE 1, got {}", v)),
            };
        }
    } else {
        return TestResult {
            name: "TTL/EXPIRE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("EXPIRE failed".to_string()),
        };
    }

    let ttl2 = redis::cmd("TTL").arg(key).query_async::<_, i64>(con).await;
    match ttl2 {
        Ok(v) if v >= 0 => TestResult {
            name: "TTL/EXPIRE Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(v) => TestResult {
            name: "TTL/EXPIRE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected TTL >= 0, got {}", v)),
        },
        Err(e) => TestResult {
            name: "TTL/EXPIRE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_rename(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running RENAME command test");
    let src = "test_rename_src";
    let dst = "test_rename_dst";
    let _ = con.del::<_, i32>(src).await;
    let _ = con.del::<_, i32>(dst).await;
    let _ = con.set::<_, _, ()>(src, "v").await;

    match redis::cmd("RENAME")
        .arg(src)
        .arg(dst)
        .query_async::<_, String>(con)
        .await
    {
        Ok(s) if s.to_uppercase() == "OK" => {
            let exists_src = con.exists::<_, i32>(src).await.unwrap_or(0);
            let exists_dst = con.exists::<_, i32>(dst).await.unwrap_or(0);
            if exists_src == 0 && exists_dst == 1 {
                TestResult {
                    name: "RENAME Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "RENAME Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!(
                        "Post-rename exists mismatch: src {}, dst {}",
                        exists_src, exists_dst
                    )),
                }
            }
        }
        Ok(s) => TestResult {
            name: "RENAME Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected OK, got {}", s)),
        },
        Err(e) => TestResult {
            name: "RENAME Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_type(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running TYPE command test");
    let key = "test_type_key";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "v").await;

    match redis::cmd("TYPE")
        .arg(key)
        .query_async::<_, String>(con)
        .await
    {
        Ok(t) if t == "string" => {
            match redis::cmd("TYPE")
                .arg("nonexistent:key")
                .query_async::<_, String>(con)
                .await
            {
                Ok(tt) if tt == "none" => TestResult {
                    name: "TYPE Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                },
                Ok(tt) => TestResult {
                    name: "TYPE Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Expected 'none' for missing, got '{}'", tt)),
                },
                Err(e) => TestResult {
                    name: "TYPE Command".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(e.to_string()),
                },
            }
        }
        Ok(t) => TestResult {
            name: "TYPE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 'string', got '{}'", t)),
        },
        Err(e) => TestResult {
            name: "TYPE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

// ---- Server command tests ----
async fn test_ping(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running PING command test");
    match redis::cmd("PING").query_async::<_, String>(con).await {
        Ok(s) if s.to_uppercase() == "PONG" => TestResult {
            name: "PING Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(s) => TestResult {
            name: "PING Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected PONG, got {}", s)),
        },
        Err(e) => TestResult {
            name: "PING Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_echo(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running ECHO command test");
    match redis::cmd("ECHO")
        .arg("hello")
        .query_async::<_, String>(con)
        .await
    {
        Ok(s) if s == "hello" => TestResult {
            name: "ECHO Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(s) => TestResult {
            name: "ECHO Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected 'hello', got '{}'", s)),
        },
        Err(e) => TestResult {
            name: "ECHO Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_info(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running INFO command test");
    match redis::cmd("INFO").query_async::<_, String>(con).await {
        Ok(s) if !s.is_empty() => TestResult {
            name: "INFO Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(_) => TestResult {
            name: "INFO Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("Empty INFO response".to_string()),
        },
        Err(e) => TestResult {
            name: "INFO Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_save(con: &mut redis::aio::Connection) -> TestResult {
    debug!("Running SAVE command test");
    match redis::cmd("SAVE").query_async::<_, String>(con).await {
        Ok(s) if s.to_uppercase() == "OK" => TestResult {
            name: "SAVE Command".to_string(),
            protocol: "Redis".to_string(),
            success: true,
            message: None,
        },
        Ok(s) => TestResult {
            name: "SAVE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Expected OK, got {}", s)),
        },
        Err(e) => TestResult {
            name: "SAVE Command".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(e.to_string()),
        },
    }
}

async fn test_pubsub(host: &str, port: u16) -> TestResult {
    debug!("Running PubSub test");
    let channel = "test_pubsub_channel";
    let message = "hello_pubsub";

    let sub_client = match redis::Client::open(format!("redis://{}:{}", host, port)) {
        Ok(c) => c,
        Err(e) => {
            return TestResult {
                name: "PubSub".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Failed to create subscriber client: {}", e)),
            };
        }
    };

    let pub_client = match redis::Client::open(format!("redis://{}:{}", host, port)) {
        Ok(c) => c,
        Err(e) => {
            return TestResult {
                name: "PubSub".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Failed to create publisher client: {}", e)),
            };
        }
    };

    let sub_con = match sub_client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            return TestResult {
                name: "PubSub".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Subscriber connection failed: {}", e)),
            };
        }
    };

    let mut pubsub = sub_con.into_pubsub();
    if let Err(e) = pubsub.subscribe(channel).await {
        return TestResult {
            name: "PubSub".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Subscribe failed: {}", e)),
        };
    }

    let mut stream = pubsub.on_message();

    // Publish from a separate connection
    let mut pub_con = match pub_client.get_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            return TestResult {
                name: "PubSub".to_string(),
                protocol: "Redis".to_string(),
                success: false,
                message: Some(format!("Publisher connection failed: {}", e)),
            };
        }
    };

    // Small delay to ensure subscription is active
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    if let Err(e) = pub_con.publish::<_, _, i64>(channel, message).await {
        return TestResult {
            name: "PubSub".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some(format!("Publish failed: {}", e)),
        };
    }

    // Wait for the message with a timeout
    match tokio::time::timeout(std::time::Duration::from_secs(5), stream.next()).await {
        Ok(Some(msg)) => {
            let payload: String = msg.get_payload().unwrap_or_default();
            if payload == message {
                TestResult {
                    name: "PubSub".to_string(),
                    protocol: "Redis".to_string(),
                    success: true,
                    message: None,
                }
            } else {
                TestResult {
                    name: "PubSub".to_string(),
                    protocol: "Redis".to_string(),
                    success: false,
                    message: Some(format!("Expected '{}', got '{}'", message, payload)),
                }
            }
        }
        Ok(None) => TestResult {
            name: "PubSub".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("Stream ended without message".to_string()),
        },
        Err(_) => TestResult {
            name: "PubSub".to_string(),
            protocol: "Redis".to_string(),
            success: false,
            message: Some("Timed out waiting for PubSub message".to_string()),
        },
    }
}

// ---- New String command tests ----

async fn test_incr_decr(con: &mut redis::aio::Connection) -> TestResult {
    let name = "INCR/DECR Command";
    let key = "test_incr_decr";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "10").await;

    let v: i64 = match redis::cmd("INCR").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("INCR failed: {}", e)),
    };
    if v != 11 {
        return fail!(name, format!("Expected 11 after INCR, got {}", v));
    }
    let v: i64 = match redis::cmd("DECR").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("DECR failed: {}", e)),
    };
    if v != 10 {
        return fail!(name, format!("Expected 10 after DECR, got {}", v));
    }
    ok!(name)
}

async fn test_incrby_decrby(con: &mut redis::aio::Connection) -> TestResult {
    let name = "INCRBY/DECRBY Command";
    let key = "test_incrby";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "100").await;

    let v: i64 = match redis::cmd("INCRBY").arg(key).arg(25).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("INCRBY failed: {}", e)),
    };
    if v != 125 {
        return fail!(name, format!("Expected 125, got {}", v));
    }
    let v: i64 = match redis::cmd("DECRBY").arg(key).arg(50).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("DECRBY failed: {}", e)),
    };
    if v != 75 {
        return fail!(name, format!("Expected 75, got {}", v));
    }
    ok!(name)
}

async fn test_incrbyfloat(con: &mut redis::aio::Connection) -> TestResult {
    let name = "INCRBYFLOAT Command";
    let key = "test_incrbyfloat";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "10.5").await;

    let v: String = match redis::cmd("INCRBYFLOAT")
        .arg(key)
        .arg("0.1")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("INCRBYFLOAT failed: {}", e)),
    };
    let parsed: f64 = v.parse().unwrap_or(0.0);
    if (parsed - 10.6).abs() > 0.001 {
        return fail!(name, format!("Expected ~10.6, got {}", v));
    }
    ok!(name)
}

async fn test_mget_mset(con: &mut redis::aio::Connection) -> TestResult {
    let name = "MGET/MSET Command";
    match redis::cmd("MSET")
        .arg("mset_a")
        .arg("1")
        .arg("mset_b")
        .arg("2")
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("MSET failed: {}", e)),
    }

    match redis::cmd("MGET")
        .arg("mset_a")
        .arg("mset_b")
        .arg("mset_nonexist")
        .query_async::<_, Vec<Option<String>>>(con)
        .await
    {
        Ok(vals) => {
            if vals.len() == 3
                && vals[0] == Some("1".to_string())
                && vals[1] == Some("2".to_string())
                && vals[2].is_none()
            {
                ok!(name)
            } else {
                fail!(name, format!("Unexpected MGET result: {:?}", vals))
            }
        }
        Err(e) => fail!(name, format!("MGET failed: {}", e)),
    }
}

async fn test_msetnx(con: &mut redis::aio::Connection) -> TestResult {
    let name = "MSETNX Command";
    let _ = con.del::<_, i32>("msetnx_a").await;
    let _ = con.del::<_, i32>("msetnx_b").await;

    let v: i64 = match redis::cmd("MSETNX")
        .arg("msetnx_a")
        .arg("1")
        .arg("msetnx_b")
        .arg("2")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("MSETNX failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1 (all set), got {}", v));
    }
    // Second call should fail since keys exist
    let v2: i64 = match redis::cmd("MSETNX")
        .arg("msetnx_a")
        .arg("3")
        .arg("msetnx_c")
        .arg("4")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("MSETNX 2nd call failed: {}", e)),
    };
    if v2 != 0 {
        return fail!(name, format!("Expected 0 (not set), got {}", v2));
    }
    ok!(name)
}

async fn test_setnx(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SETNX Command";
    let key = "test_setnx";
    let _ = con.del::<_, i32>(key).await;

    let v: i64 = match redis::cmd("SETNX")
        .arg(key)
        .arg("val")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SETNX failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1, got {}", v));
    }
    let v2: i64 = match redis::cmd("SETNX")
        .arg(key)
        .arg("other")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SETNX 2nd call failed: {}", e)),
    };
    if v2 != 0 {
        return fail!(name, format!("Expected 0, got {}", v2));
    }
    ok!(name)
}

async fn test_setex(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SETEX Command";
    let key = "test_setex";
    let _ = con.del::<_, i32>(key).await;

    match redis::cmd("SETEX")
        .arg(key)
        .arg(10)
        .arg("val")
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("SETEX failed: {}", e)),
    }
    let ttl: i64 = match redis::cmd("TTL").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("TTL failed: {}", e)),
    };
    if ttl <= 0 || ttl > 10 {
        return fail!(name, format!("Expected TTL 1-10, got {}", ttl));
    }
    ok!(name)
}

async fn test_getrange(con: &mut redis::aio::Connection) -> TestResult {
    let name = "GETRANGE Command";
    let key = "test_getrange";
    let _ = con.set::<_, _, ()>(key, "Hello, World!").await;

    let v: String = match redis::cmd("GETRANGE")
        .arg(key)
        .arg(0)
        .arg(4)
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("GETRANGE failed: {}", e)),
    };
    if v != "Hello" {
        return fail!(name, format!("Expected 'Hello', got '{}'", v));
    }
    ok!(name)
}

async fn test_setrange(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SETRANGE Command";
    let key = "test_setrange";
    let _ = con.set::<_, _, ()>(key, "Hello World").await;

    let v: i64 = match redis::cmd("SETRANGE")
        .arg(key)
        .arg(6)
        .arg("Redis")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SETRANGE failed: {}", e)),
    };
    if v != 11 {
        return fail!(name, format!("Expected length 11, got {}", v));
    }
    let val: String = con.get(key).await.unwrap_or_default();
    if val != "Hello Redis" {
        return fail!(name, format!("Expected 'Hello Redis', got '{}'", val));
    }
    ok!(name)
}

async fn test_getdel(con: &mut redis::aio::Connection) -> TestResult {
    let name = "GETDEL Command";
    let key = "test_getdel";
    let _ = con.set::<_, _, ()>(key, "myval").await;

    let v: String = match redis::cmd("GETDEL").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("GETDEL failed: {}", e)),
    };
    if v != "myval" {
        return fail!(name, format!("Expected 'myval', got '{}'", v));
    }
    let exists: i32 = con.exists(key).await.unwrap_or(1);
    if exists != 0 {
        return fail!(name, "Key should have been deleted");
    }
    ok!(name)
}

// ---- New Key command tests ----

async fn test_persist(con: &mut redis::aio::Connection) -> TestResult {
    let name = "PERSIST Command";
    let key = "test_persist";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "v").await;
    let _ = redis::cmd("EXPIRE")
        .arg(key)
        .arg(100)
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("PERSIST").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("PERSIST failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1, got {}", v));
    }
    let ttl: i64 = redis::cmd("TTL")
        .arg(key)
        .query_async(con)
        .await
        .unwrap_or(0);
    if ttl != -1 {
        return fail!(name, format!("Expected TTL -1 after persist, got {}", ttl));
    }
    ok!(name)
}

async fn test_pttl_pexpire(con: &mut redis::aio::Connection) -> TestResult {
    let name = "PTTL/PEXPIRE Command";
    let key = "test_pttl";
    let _ = con.del::<_, i32>(key).await;
    let _ = con.set::<_, _, ()>(key, "v").await;

    let _ = redis::cmd("PEXPIRE")
        .arg(key)
        .arg(10000)
        .query_async::<_, i64>(con)
        .await;
    let pttl: i64 = match redis::cmd("PTTL").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("PTTL failed: {}", e)),
    };
    if pttl <= 0 || pttl > 10000 {
        return fail!(name, format!("Expected PTTL 1-10000, got {}", pttl));
    }
    ok!(name)
}

async fn test_unlink(con: &mut redis::aio::Connection) -> TestResult {
    let name = "UNLINK Command";
    let key = "test_unlink";
    let _ = con.set::<_, _, ()>(key, "v").await;

    let v: i64 = match redis::cmd("UNLINK").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("UNLINK failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1, got {}", v));
    }
    let exists: i32 = con.exists(key).await.unwrap_or(1);
    if exists != 0 {
        return fail!(name, "Key should not exist after UNLINK");
    }
    ok!(name)
}

async fn test_renamenx(con: &mut redis::aio::Connection) -> TestResult {
    let name = "RENAMENX Command";
    let _ = con.del::<_, i32>("rnx_src").await;
    let _ = con.del::<_, i32>("rnx_dst").await;
    let _ = con.set::<_, _, ()>("rnx_src", "v").await;

    let v: i64 = match redis::cmd("RENAMENX")
        .arg("rnx_src")
        .arg("rnx_dst")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("RENAMENX failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1, got {}", v));
    }
    // Now set dst again and try rename to existing key
    let _ = con.set::<_, _, ()>("rnx_src2", "v2").await;
    let v2: i64 = match redis::cmd("RENAMENX")
        .arg("rnx_src2")
        .arg("rnx_dst")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("RENAMENX 2nd call failed: {}", e)),
    };
    if v2 != 0 {
        return fail!(name, format!("Expected 0 (dst exists), got {}", v2));
    }
    ok!(name)
}

async fn test_copy(con: &mut redis::aio::Connection) -> TestResult {
    let name = "COPY Command";
    let _ = con.del::<_, i32>("copy_src").await;
    let _ = con.del::<_, i32>("copy_dst").await;
    let _ = con.set::<_, _, ()>("copy_src", "hello").await;

    let v: i64 = match redis::cmd("COPY")
        .arg("copy_src")
        .arg("copy_dst")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("COPY failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1, got {}", v));
    }
    let val: String = con.get("copy_dst").await.unwrap_or_default();
    if val != "hello" {
        return fail!(name, format!("Expected 'hello', got '{}'", val));
    }
    ok!(name)
}

async fn test_touch(con: &mut redis::aio::Connection) -> TestResult {
    let name = "TOUCH Command";
    let _ = con.set::<_, _, ()>("touch_a", "1").await;
    let _ = con.set::<_, _, ()>("touch_b", "2").await;

    let v: i64 = match redis::cmd("TOUCH")
        .arg("touch_a")
        .arg("touch_b")
        .arg("touch_nonexist")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("TOUCH failed: {}", e)),
    };
    if v != 2 {
        return fail!(name, format!("Expected 2, got {}", v));
    }
    ok!(name)
}

async fn test_scan(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SCAN Command";
    let _ = con.set::<_, _, ()>("scan_test_a", "1").await;
    let _ = con.set::<_, _, ()>("scan_test_b", "2").await;

    let result: (i64, Vec<String>) = match redis::cmd("SCAN")
        .arg(0)
        .arg("MATCH")
        .arg("scan_test_*")
        .arg("COUNT")
        .arg(100)
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SCAN failed: {}", e)),
    };
    let (_cursor, keys) = result;
    if keys.len() < 2 {
        return fail!(name, format!("Expected >= 2 keys, got {}", keys.len()));
    }
    ok!(name)
}

async fn test_dbsize(con: &mut redis::aio::Connection) -> TestResult {
    let name = "DBSIZE Command";
    let v: i64 = match redis::cmd("DBSIZE").query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("DBSIZE failed: {}", e)),
    };
    if v < 0 {
        return fail!(name, format!("DBSIZE returned negative: {}", v));
    }
    ok!(name)
}

async fn test_time(con: &mut redis::aio::Connection) -> TestResult {
    let name = "TIME Command";
    let v: Vec<String> = match redis::cmd("TIME").query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("TIME failed: {}", e)),
    };
    if v.len() != 2 {
        return fail!(name, format!("Expected 2 elements, got {}", v.len()));
    }
    ok!(name)
}

// ---- New List command tests ----

async fn test_lindex(con: &mut redis::aio::Connection) -> TestResult {
    let name = "LINDEX Command";
    let key = "test_lindex";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: String = match redis::cmd("LINDEX").arg(key).arg(1).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("LINDEX failed: {}", e)),
    };
    if v != "b" {
        return fail!(name, format!("Expected 'b', got '{}'", v));
    }
    // Negative index
    let v2: String = match redis::cmd("LINDEX").arg(key).arg(-1).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("LINDEX -1 failed: {}", e)),
    };
    if v2 != "c" {
        return fail!(name, format!("Expected 'c', got '{}'", v2));
    }
    ok!(name)
}

async fn test_lset(con: &mut redis::aio::Connection) -> TestResult {
    let name = "LSET Command";
    let key = "test_lset";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("LSET")
        .arg(key)
        .arg(1)
        .arg("B")
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("LSET failed: {}", e)),
    }
    let v: String = match redis::cmd("LINDEX").arg(key).arg(1).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("LINDEX after LSET failed: {}", e)),
    };
    if v != "B" {
        return fail!(name, format!("Expected 'B', got '{}'", v));
    }
    ok!(name)
}

async fn test_ltrim(con: &mut redis::aio::Connection) -> TestResult {
    let name = "LTRIM Command";
    let key = "test_ltrim";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("LTRIM")
        .arg(key)
        .arg(1)
        .arg(2)
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("LTRIM failed: {}", e)),
    }
    let vals: Vec<String> = redis::cmd("LRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .query_async(con)
        .await
        .unwrap_or_default();
    if vals != vec!["b", "c"] {
        return fail!(name, format!("Expected [b,c], got {:?}", vals));
    }
    ok!(name)
}

async fn test_linsert(con: &mut redis::aio::Connection) -> TestResult {
    let name = "LINSERT Command";
    let key = "test_linsert";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("LINSERT")
        .arg(key)
        .arg("BEFORE")
        .arg("c")
        .arg("b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("LINSERT failed: {}", e)),
    };
    if v != 3 {
        return fail!(name, format!("Expected length 3, got {}", v));
    }
    let vals: Vec<String> = redis::cmd("LRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .query_async(con)
        .await
        .unwrap_or_default();
    if vals != vec!["a", "b", "c"] {
        return fail!(name, format!("Expected [a,b,c], got {:?}", vals));
    }
    ok!(name)
}

async fn test_lrem(con: &mut redis::aio::Connection) -> TestResult {
    let name = "LREM Command";
    let key = "test_lrem";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("a")
        .arg("c")
        .arg("a")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("LREM")
        .arg(key)
        .arg(2)
        .arg("a")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("LREM failed: {}", e)),
    };
    if v != 2 {
        return fail!(name, format!("Expected 2 removed, got {}", v));
    }
    let vals: Vec<String> = redis::cmd("LRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .query_async(con)
        .await
        .unwrap_or_default();
    if vals != vec!["b", "c", "a"] {
        return fail!(name, format!("Expected [b,c,a], got {:?}", vals));
    }
    ok!(name)
}

async fn test_lpushx_rpushx(con: &mut redis::aio::Connection) -> TestResult {
    let name = "LPUSHX/RPUSHX Command";
    let key = "test_pushx";
    let _ = con.del::<_, i32>(key).await;

    // LPUSHX on non-existent key should return 0
    let v: i64 = match redis::cmd("LPUSHX")
        .arg(key)
        .arg("val")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("LPUSHX failed: {}", e)),
    };
    if v != 0 {
        return fail!(name, format!("Expected 0 for non-existent key, got {}", v));
    }
    // Create list then RPUSHX
    let _ = redis::cmd("RPUSH")
        .arg(key)
        .arg("a")
        .query_async::<_, i64>(con)
        .await;
    let v2: i64 = match redis::cmd("RPUSHX")
        .arg(key)
        .arg("b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("RPUSHX failed: {}", e)),
    };
    if v2 != 2 {
        return fail!(name, format!("Expected 2, got {}", v2));
    }
    ok!(name)
}

// ---- New Set command tests ----

async fn test_spop(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SPOP Command";
    let key = "test_spop";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("SADD")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: String = match redis::cmd("SPOP").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SPOP failed: {}", e)),
    };
    if !["a", "b", "c"].contains(&v.as_str()) {
        return fail!(name, format!("Unexpected value: {}", v));
    }
    let card: i64 = redis::cmd("SCARD")
        .arg(key)
        .query_async(con)
        .await
        .unwrap_or(0);
    if card != 2 {
        return fail!(name, format!("Expected 2 remaining, got {}", card));
    }
    ok!(name)
}

async fn test_smove(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SMOVE Command";
    let _ = con.del::<_, i32>("smove_src").await;
    let _ = con.del::<_, i32>("smove_dst").await;
    let _ = redis::cmd("SADD")
        .arg("smove_src")
        .arg("a")
        .arg("b")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SADD")
        .arg("smove_dst")
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("SMOVE")
        .arg("smove_src")
        .arg("smove_dst")
        .arg("a")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SMOVE failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1, got {}", v));
    }
    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("smove_dst")
        .arg("a")
        .query_async(con)
        .await
        .unwrap_or(0);
    if is_member != 1 {
        return fail!(name, "'a' should be in destination set");
    }
    ok!(name)
}

async fn test_sinter(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SINTER Command";
    let _ = con.del::<_, i32>("si_a").await;
    let _ = con.del::<_, i32>("si_b").await;
    let _ = redis::cmd("SADD")
        .arg("si_a")
        .arg("1")
        .arg("2")
        .arg("3")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SADD")
        .arg("si_b")
        .arg("2")
        .arg("3")
        .arg("4")
        .query_async::<_, i64>(con)
        .await;

    let v: Vec<String> = match redis::cmd("SINTER")
        .arg("si_a")
        .arg("si_b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SINTER failed: {}", e)),
    };
    let set: HashSet<String> = v.into_iter().collect();
    let expected: HashSet<String> = ["2", "3"].iter().map(|s| s.to_string()).collect();
    if set != expected {
        return fail!(name, format!("Expected {{2,3}}, got {:?}", set));
    }
    ok!(name)
}

async fn test_sunion(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SUNION Command";
    let _ = con.del::<_, i32>("su_a").await;
    let _ = con.del::<_, i32>("su_b").await;
    let _ = redis::cmd("SADD")
        .arg("su_a")
        .arg("1")
        .arg("2")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SADD")
        .arg("su_b")
        .arg("2")
        .arg("3")
        .query_async::<_, i64>(con)
        .await;

    let v: Vec<String> = match redis::cmd("SUNION")
        .arg("su_a")
        .arg("su_b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SUNION failed: {}", e)),
    };
    let set: HashSet<String> = v.into_iter().collect();
    if set.len() != 3 {
        return fail!(name, format!("Expected 3 elements, got {}", set.len()));
    }
    ok!(name)
}

async fn test_sdiff(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SDIFF Command";
    let _ = con.del::<_, i32>("sd_a").await;
    let _ = con.del::<_, i32>("sd_b").await;
    let _ = redis::cmd("SADD")
        .arg("sd_a")
        .arg("1")
        .arg("2")
        .arg("3")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SADD")
        .arg("sd_b")
        .arg("2")
        .arg("3")
        .arg("4")
        .query_async::<_, i64>(con)
        .await;

    let v: Vec<String> = match redis::cmd("SDIFF")
        .arg("sd_a")
        .arg("sd_b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SDIFF failed: {}", e)),
    };
    if v != vec!["1"] {
        return fail!(name, format!("Expected [1], got {:?}", v));
    }
    ok!(name)
}

async fn test_sinterstore(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SINTERSTORE Command";
    let _ = con.del::<_, i32>("sis_a").await;
    let _ = con.del::<_, i32>("sis_b").await;
    let _ = con.del::<_, i32>("sis_dst").await;
    let _ = redis::cmd("SADD")
        .arg("sis_a")
        .arg("1")
        .arg("2")
        .arg("3")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SADD")
        .arg("sis_b")
        .arg("2")
        .arg("3")
        .arg("4")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("SINTERSTORE")
        .arg("sis_dst")
        .arg("sis_a")
        .arg("sis_b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SINTERSTORE failed: {}", e)),
    };
    if v != 2 {
        return fail!(name, format!("Expected 2, got {}", v));
    }
    ok!(name)
}

// ---- New Hash command tests ----

async fn test_hmget(con: &mut redis::aio::Connection) -> TestResult {
    let name = "HMGET Command";
    let key = "test_hmget";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query_async::<_, i64>(con)
        .await;

    let v: Vec<Option<String>> = match redis::cmd("HMGET")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("HMGET failed: {}", e)),
    };
    if v.len() != 3
        || v[0] != Some("1".to_string())
        || v[1] != Some("2".to_string())
        || v[2].is_some()
    {
        return fail!(name, format!("Unexpected HMGET result: {:?}", v));
    }
    ok!(name)
}

async fn test_hincrby(con: &mut redis::aio::Connection) -> TestResult {
    let name = "HINCRBY Command";
    let key = "test_hincrby";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("count")
        .arg("10")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("HINCRBY")
        .arg(key)
        .arg("count")
        .arg(5)
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("HINCRBY failed: {}", e)),
    };
    if v != 15 {
        return fail!(name, format!("Expected 15, got {}", v));
    }
    ok!(name)
}

async fn test_hsetnx(con: &mut redis::aio::Connection) -> TestResult {
    let name = "HSETNX Command";
    let key = "test_hsetnx";
    let _ = con.del::<_, i32>(key).await;

    let v: i64 = match redis::cmd("HSETNX")
        .arg(key)
        .arg("field")
        .arg("val")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("HSETNX failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1 (new field), got {}", v));
    }
    let v2: i64 = match redis::cmd("HSETNX")
        .arg(key)
        .arg("field")
        .arg("other")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("HSETNX 2nd call failed: {}", e)),
    };
    if v2 != 0 {
        return fail!(name, format!("Expected 0 (existing field), got {}", v2));
    }
    ok!(name)
}

async fn test_hstrlen(con: &mut redis::aio::Connection) -> TestResult {
    let name = "HSTRLEN Command";
    let key = "test_hstrlen";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("HSET")
        .arg(key)
        .arg("field")
        .arg("hello")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("HSTRLEN")
        .arg(key)
        .arg("field")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("HSTRLEN failed: {}", e)),
    };
    if v != 5 {
        return fail!(name, format!("Expected 5, got {}", v));
    }
    ok!(name)
}

// ---- Sorted Set command tests ----

async fn test_zadd_zscore(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZADD/ZSCORE Command";
    let key = "test_zset";
    let _ = con.del::<_, i32>(key).await;

    let v: i64 = match redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZADD failed: {}", e)),
    };
    if v != 3 {
        return fail!(name, format!("Expected 3 added, got {}", v));
    }

    let score: String = match redis::cmd("ZSCORE")
        .arg(key)
        .arg("b")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZSCORE failed: {}", e)),
    };
    let parsed: f64 = score.parse().unwrap_or(0.0);
    if (parsed - 2.0).abs() > 0.001 {
        return fail!(name, format!("Expected score 2.0, got {}", score));
    }
    ok!(name)
}

async fn test_zcard(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZCARD Command";
    let key = "test_zcard";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("ZCARD").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZCARD failed: {}", e)),
    };
    if v != 2 {
        return fail!(name, format!("Expected 2, got {}", v));
    }
    ok!(name)
}

async fn test_zrank(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZRANK Command";
    let key = "test_zrank";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("ZRANK").arg(key).arg("b").query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZRANK failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected rank 1, got {}", v));
    }
    ok!(name)
}

async fn test_zrange(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZRANGE Command";
    let key = "test_zrange";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: Vec<String> = match redis::cmd("ZRANGE")
        .arg(key)
        .arg(0)
        .arg(-1)
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZRANGE failed: {}", e)),
    };
    if v != vec!["a", "b", "c"] {
        return fail!(name, format!("Expected [a,b,c], got {:?}", v));
    }
    ok!(name)
}

async fn test_zrem(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZREM Command";
    let key = "test_zrem";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("ZREM").arg(key).arg("a").query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZREM failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1 removed, got {}", v));
    }
    let card: i64 = redis::cmd("ZCARD")
        .arg(key)
        .query_async(con)
        .await
        .unwrap_or(0);
    if card != 1 {
        return fail!(name, format!("Expected card 1, got {}", card));
    }
    ok!(name)
}

async fn test_zincrby(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZINCRBY Command";
    let key = "test_zincrby";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(5.0)
        .arg("member")
        .query_async::<_, i64>(con)
        .await;

    let v: String = match redis::cmd("ZINCRBY")
        .arg(key)
        .arg(3.0)
        .arg("member")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZINCRBY failed: {}", e)),
    };
    let parsed: f64 = v.parse().unwrap_or(0.0);
    if (parsed - 8.0).abs() > 0.001 {
        return fail!(name, format!("Expected 8.0, got {}", v));
    }
    ok!(name)
}

async fn test_zcount(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZCOUNT Command";
    let key = "test_zcount";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .arg(4.0)
        .arg("d")
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("ZCOUNT")
        .arg(key)
        .arg("2")
        .arg("3")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZCOUNT failed: {}", e)),
    };
    if v != 2 {
        return fail!(name, format!("Expected 2, got {}", v));
    }
    ok!(name)
}

async fn test_zpopmin_zpopmax(con: &mut redis::aio::Connection) -> TestResult {
    let name = "ZPOPMIN/ZPOPMAX Command";
    let key = "test_zpop";
    let _ = con.del::<_, i32>(key).await;
    let _ = redis::cmd("ZADD")
        .arg(key)
        .arg(1.0)
        .arg("a")
        .arg(2.0)
        .arg("b")
        .arg(3.0)
        .arg("c")
        .query_async::<_, i64>(con)
        .await;

    let v: Vec<String> = match redis::cmd("ZPOPMIN").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZPOPMIN failed: {}", e)),
    };
    if v.is_empty() || v[0] != "a" {
        return fail!(name, format!("Expected 'a' from ZPOPMIN, got {:?}", v));
    }

    let v2: Vec<String> = match redis::cmd("ZPOPMAX").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("ZPOPMAX failed: {}", e)),
    };
    if v2.is_empty() || v2[0] != "c" {
        return fail!(name, format!("Expected 'c' from ZPOPMAX, got {:?}", v2));
    }
    ok!(name)
}

// ---- Bitmap command tests ----

async fn test_setbit_getbit(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SETBIT/GETBIT Command";
    let key = "test_bitmap";
    let _ = con.del::<_, i32>(key).await;

    let v: i64 = match redis::cmd("SETBIT")
        .arg(key)
        .arg(7)
        .arg(1)
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("SETBIT failed: {}", e)),
    };
    if v != 0 {
        return fail!(name, format!("Expected old bit 0, got {}", v));
    }

    let v2: i64 = match redis::cmd("GETBIT").arg(key).arg(7).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("GETBIT failed: {}", e)),
    };
    if v2 != 1 {
        return fail!(name, format!("Expected bit 1, got {}", v2));
    }
    ok!(name)
}

async fn test_bitcount(con: &mut redis::aio::Connection) -> TestResult {
    let name = "BITCOUNT Command";
    let key = "test_bitcount";
    let _ = con.del::<_, i32>(key).await;
    // Set a few bits
    let _ = redis::cmd("SETBIT")
        .arg(key)
        .arg(0)
        .arg(1)
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SETBIT")
        .arg(key)
        .arg(1)
        .arg(1)
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("SETBIT")
        .arg(key)
        .arg(2)
        .arg(1)
        .query_async::<_, i64>(con)
        .await;

    let v: i64 = match redis::cmd("BITCOUNT").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("BITCOUNT failed: {}", e)),
    };
    if v != 3 {
        return fail!(name, format!("Expected 3, got {}", v));
    }
    ok!(name)
}

// ---- HyperLogLog command tests ----

async fn test_pfadd_pfcount(con: &mut redis::aio::Connection) -> TestResult {
    let name = "PFADD/PFCOUNT Command";
    let key = "test_hll";
    let _ = con.del::<_, i32>(key).await;

    let v: i64 = match redis::cmd("PFADD")
        .arg(key)
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("PFADD failed: {}", e)),
    };
    if v != 1 {
        return fail!(name, format!("Expected 1 (changed), got {}", v));
    }

    let count: i64 = match redis::cmd("PFCOUNT").arg(key).query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("PFCOUNT failed: {}", e)),
    };
    if count != 3 {
        return fail!(name, format!("Expected count 3, got {}", count));
    }
    ok!(name)
}

async fn test_pfmerge(con: &mut redis::aio::Connection) -> TestResult {
    let name = "PFMERGE Command";
    let _ = con.del::<_, i32>("hll_a").await;
    let _ = con.del::<_, i32>("hll_b").await;
    let _ = con.del::<_, i32>("hll_merged").await;
    let _ = redis::cmd("PFADD")
        .arg("hll_a")
        .arg("x")
        .arg("y")
        .query_async::<_, i64>(con)
        .await;
    let _ = redis::cmd("PFADD")
        .arg("hll_b")
        .arg("y")
        .arg("z")
        .query_async::<_, i64>(con)
        .await;

    match redis::cmd("PFMERGE")
        .arg("hll_merged")
        .arg("hll_a")
        .arg("hll_b")
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("PFMERGE failed: {}", e)),
    }

    let count: i64 = match redis::cmd("PFCOUNT")
        .arg("hll_merged")
        .query_async(con)
        .await
    {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("PFCOUNT after merge failed: {}", e)),
    };
    if count != 3 {
        return fail!(name, format!("Expected 3 unique, got {}", count));
    }
    ok!(name)
}

// ---- Transaction command tests ----

async fn test_multi_exec(con: &mut redis::aio::Connection) -> TestResult {
    let name = "MULTI/EXEC Command";
    let _ = con.del::<_, i32>("multi_key").await;

    match redis::cmd("MULTI").query_async::<_, String>(con).await {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("MULTI failed: {}", e)),
    }
    match redis::cmd("SET")
        .arg("multi_key")
        .arg("txn_value")
        .query_async::<_, String>(con)
        .await
    {
        Ok(s) if s == "QUEUED" => {}
        Ok(s) => return fail!(name, format!("Expected QUEUED, got {}", s)),
        Err(e) => return fail!(name, format!("SET in MULTI failed: {}", e)),
    }
    match redis::cmd("GET")
        .arg("multi_key")
        .query_async::<_, String>(con)
        .await
    {
        Ok(s) if s == "QUEUED" => {}
        Ok(s) => return fail!(name, format!("Expected QUEUED for GET, got {}", s)),
        Err(e) => return fail!(name, format!("GET in MULTI failed: {}", e)),
    }

    // EXEC returns array of results
    let results: Vec<redis::Value> = match redis::cmd("EXEC").query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("EXEC failed: {}", e)),
    };
    if results.len() != 2 {
        return fail!(name, format!("Expected 2 results, got {}", results.len()));
    }
    ok!(name)
}

async fn test_multi_discard(con: &mut redis::aio::Connection) -> TestResult {
    let name = "MULTI/DISCARD Command";

    match redis::cmd("MULTI").query_async::<_, String>(con).await {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("MULTI failed: {}", e)),
    }
    let _ = redis::cmd("SET")
        .arg("discard_key")
        .arg("val")
        .query_async::<_, String>(con)
        .await;

    match redis::cmd("DISCARD").query_async::<_, String>(con).await {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("DISCARD failed: {}", e)),
    }

    // After DISCARD, we should not be in MULTI mode
    // Normal commands should work
    let _ = con.set::<_, _, ()>("discard_verify", "ok").await;
    let v: String = con.get("discard_verify").await.unwrap_or_default();
    if v != "ok" {
        return fail!(name, "Commands after DISCARD should work normally");
    }
    ok!(name)
}

// ---- Server command tests ----

async fn test_config(con: &mut redis::aio::Connection) -> TestResult {
    let name = "CONFIG Command";
    match redis::cmd("CONFIG")
        .arg("SET")
        .arg("foo")
        .arg("bar")
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => {}
        Err(e) => return fail!(name, format!("CONFIG SET failed: {}", e)),
    }
    ok!(name)
}

async fn test_command_cmd(con: &mut redis::aio::Connection) -> TestResult {
    let name = "COMMAND Command";
    let v: i64 = match redis::cmd("COMMAND").arg("COUNT").query_async(con).await {
        Ok(v) => v,
        Err(e) => return fail!(name, format!("COMMAND COUNT failed: {}", e)),
    };
    if v <= 0 {
        return fail!(name, format!("Expected positive count, got {}", v));
    }
    ok!(name)
}

async fn test_select(con: &mut redis::aio::Connection) -> TestResult {
    let name = "SELECT Command";
    match redis::cmd("SELECT")
        .arg(0)
        .query_async::<_, String>(con)
        .await
    {
        Ok(_) => ok!(name),
        Err(e) => fail!(name, format!("SELECT 0 failed: {}", e)),
    }
}

async fn test_flushdb(con: &mut redis::aio::Connection) -> TestResult {
    let name = "FLUSHDB Command";
    // Set a key, flush, verify it's gone
    let _ = con.set::<_, _, ()>("flush_test_key", "v").await;
    match redis::cmd("FLUSHDB").query_async::<_, String>(con).await {
        Ok(_) => {
            let exists: i32 = con.exists("flush_test_key").await.unwrap_or(1);
            if exists != 0 {
                return fail!(name, "Key should not exist after FLUSHDB");
            }
            ok!(name)
        }
        Err(e) => fail!(name, format!("FLUSHDB failed: {}", e)),
    }
}

async fn test_concurrent_connections(host: &str, port: u16) -> TestResult {
    debug!("Running concurrent connections test");
    let num_tasks = 10;
    let mut handles = Vec::new();

    for i in 0..num_tasks {
        let host = host.to_string();
        let handle = tokio::spawn(async move {
            let client = redis::Client::open(format!("redis://{}:{}", host, port))?;
            let mut con = client.get_async_connection().await?;
            let key = format!("concurrent_test_{}", i);
            let value = format!("value_{}", i);
            con.set::<_, _, ()>(&key, &value).await?;
            let got: String = con.get(&key).await?;
            con.del::<_, i32>(&key).await?;
            if got == value {
                Ok::<bool, redis::RedisError>(true)
            } else {
                Ok(false)
            }
        });
        handles.push(handle);
    }

    let mut all_ok = true;
    let mut err_msg = None;
    for handle in handles {
        match handle.await {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                all_ok = false;
                err_msg = Some("Value mismatch in concurrent connection".to_string());
            }
            Ok(Err(e)) => {
                all_ok = false;
                err_msg = Some(format!("Redis error in concurrent task: {}", e));
            }
            Err(e) => {
                all_ok = false;
                err_msg = Some(format!("Task join error: {}", e));
            }
        }
    }

    TestResult {
        name: "Concurrent Connections".to_string(),
        protocol: "Redis".to_string(),
        success: all_ok,
        message: err_msg,
    }
}
