#![feature(slice_patterns)]
use std::fs::{File, read};
use std::io::BufReader;
use std::io::prelude::*;
use redis::ToRedisArgs;

use std::process::exit;
use std::sync::atomic::AtomicU64;
use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;
use itertools::Itertools;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

const MSET_SIZE: usize = 200;

fn injector(i: u32, global_expired_count: &AtomicU64, global_inserted_count: &AtomicU64, addr: &str, r: crossbeam_channel::Receiver<Vec<(Vec<u8>,Vec<u8>,Vec<u8>)>>) -> Result<(), failure::Error> {
    let mut client = redis::Client::open(addr)?;
    let mut con = client.get_connection()?;

    let mut thread_inserted_count:u64 = 0;
    let mut thread_expired_count: u64 = 0;
    let mut iteration = 0;

    while let Ok(bulk) = r.recv() {
        let mut loop_inserted_count = 0;
        let mut loop_expired_count = 0;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let len = bulk.len();

        let mut pipe = redis::pipe();
        let mut pipe = &mut pipe;
        for (key, value, expire_date) in bulk {
            let ttl: i64 = String::from_utf8(expire_date).unwrap().parse::<i64>().unwrap() - now;
            if ttl > 0 {
                pipe = pipe.cmd("SET").arg(key).arg(value).arg("EX").arg(ttl.to_string()).ignore();
                loop_inserted_count += 1;
            } else {
                loop_expired_count += 1;
            }
        }

        loop {
            iteration += 1;
            match (pipe.query(&mut con), iteration % 100 == 0) {
                (Ok(()), false) => break,
                (Ok(()), true) => {
                    println!("thread {:>4} renewing connection", i);
                    client = if let Ok(c) = redis::Client::open(addr) {c} else {continue};
                    con = if let Ok(c) = client.get_connection() {c} else {continue};
                    continue;
                },
                (Err(e), _) => {
                    println!("thread {:>4} redis connection failed: {:?}", i, e);
                    std::thread::sleep(std::time::Duration::from_secs(2));

                    client = if let Ok(c) = redis::Client::open(addr) {c} else {continue};
                    con = if let Ok(c) = client.get_connection() {c} else {continue};
                    println!("thread {:>4} connected again", i);
                }
            }
        }

        thread_inserted_count += loop_inserted_count;
        thread_expired_count += loop_expired_count;

        let gec = global_expired_count.fetch_add(loop_expired_count as u64, std::sync::atomic::Ordering::SeqCst);
        let gic = global_inserted_count.fetch_add(loop_inserted_count as u64, std::sync::atomic::Ordering::SeqCst);
        println!("thread {:>4}: inserted {:>8}, expired {:>8} | total: inserted {:>8}, expired {:>8}", i, thread_inserted_count, thread_expired_count, gic + loop_inserted_count as u64, gec + loop_expired_count as u64);

    }

    Ok(())
}

fn main() -> Result<(), failure::Error> {

    let addr = std::env::args().nth(1).ok_or(failure::format_err!("missing redis addr"))?;
    let mut set_count = 0;
    let global_inserted_count = AtomicU64::new(0);
    let global_expired_count = AtomicU64::new(0);
    let mut hm = HashMap::new();
    crossbeam_utils::thread::scope(|scope| {

        let (s, r) = crossbeam_channel::bounded(1000  );

        for i in 0..128 {
            let r = r.clone();
            let addr = addr.clone();
            let gic = &global_inserted_count;
            let gec = &global_expired_count;
            scope.spawn(move |_| {
                injector(i, gec, gic, &addr, r).unwrap();
            });
        }

        let stdin = std::io::stdin();
        let reader = stdin.lock();
        let mut parser = redis::Parser::new(reader);

        let mut mset_vec = Vec::with_capacity(MSET_SIZE);




        while let Ok(redis::Value::Bulk(bulk)) = parser.parse_value() {
            match bulk.as_slice() {
                [redis::Value::Data(command), redis::Value::Data(key), redis::Value::Data(value)] if &*command == b"SET"  => {
                    assert_eq!(hm.insert(key.to_owned(), value.to_owned()), None);
                    set_count += 1;
                }
                [redis::Value::Data(command), redis::Value::Data(key), redis::Value::Data(expire_date)] if &*command == b"EXPIREAT"  => {
                    if let Some((key, value)) = hm.remove_entry(key) {
                        mset_vec.push((key, value, expire_date.to_owned()));
                        if mset_vec.len() == mset_vec.capacity() {
                            let mut tmp = Vec::with_capacity(MSET_SIZE);
                            std::mem::swap(&mut tmp, &mut mset_vec);
                            s.send(tmp);
                        }
                    } else {
                        println!("Got EXPIREAT of unknown key: {}", String::from_utf8_lossy(key));
                    }
                }
                [ref c @ ..] => {
                    println!("Ignored command: {:?}", c)
                }
            }
        }

        s.send(mset_vec);


    });

    // execute SET commands without EXPIREAT
    let without_ttl_count = hm.len() as u64;
    println!("There is {} keys remaining without an EXPIREAT", without_ttl_count);
    let client = redis::Client::open(addr.as_str()).unwrap();
    let mut con = client.get_connection().unwrap();
    let mut pipe = redis::pipe();
    let mut pipe = &mut pipe;
    for (key, value) in hm {
        pipe = pipe.cmd("SET").arg(key).arg(value).arg("EX").arg("3600").ignore();
    }
    pipe.query::<()>(&mut con).unwrap();
    println!("Done injecting keys without EXPIREAT");

    println!("read SET count: {}", set_count);
    println!("inserted {} + expired {} + without_ttl {} = {}", global_inserted_count.load(std::sync::atomic::Ordering::SeqCst), global_expired_count.load(std::sync::atomic::Ordering::SeqCst), without_ttl_count, global_inserted_count.load(std::sync::atomic::Ordering::SeqCst) + global_expired_count.load(std::sync::atomic::Ordering::SeqCst) + without_ttl_count);

    Ok(())
}