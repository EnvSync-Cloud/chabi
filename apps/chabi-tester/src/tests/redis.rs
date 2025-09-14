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

    Ok(results)
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
