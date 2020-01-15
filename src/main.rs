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

const MSET_SIZE: usize = 200;

fn injector(i: u32, counter: &AtomicU64, addr: &str, r: crossbeam_channel::Receiver<Vec<Vec<u8>>>) -> Result<(), failure::Error> {
    let client = redis::Client::open(addr)?;
    let mut con = client.get_connection()?;

    let mut count = 0;
    while let Ok(args) = r.recv() {
        redis::cmd("MSET").arg(args).query(&mut con)?;
        count += 1;
        let cur = counter.fetch_add(MSET_SIZE as u64, std::sync::atomic::Ordering::SeqCst);
        println!("{} -> {} * {} = {} - total: {}", i, count, MSET_SIZE, count * MSET_SIZE, cur + MSET_SIZE as u64);
    }

    Ok(())
}

fn main() -> Result<(), failure::Error> {

    let addr = std::env::args().nth(1).ok_or(failure::format_err!("missing redis addr"))?;

    let counter = AtomicU64::new(0);
    crossbeam_utils::thread::scope(|scope| {

    let (s, r) = crossbeam_channel::bounded(1000);

    for i in 0..50 {
        let r = r.clone();
        let addr = addr.clone();
        let a = &counter;
        scope.spawn(move |_| {
            injector(i, a, &addr, r).unwrap();
        });
    }

    let stdin = std::io::stdin();
    let reader = stdin.lock();
    let mut parser = redis::Parser::new(reader);

    let mut mset_vec = Vec::with_capacity(MSET_SIZE * 2);

    while let Ok(redis::Value::Bulk(bulk)) = parser.parse_value() {
        match bulk.as_slice() {
            [redis::Value::Data(command), redis::Value::Data(key), redis::Value::Data(value)] if &*command == b"SET"  => {
                if mset_vec.len() < mset_vec.capacity() {
                    mset_vec.push(key.to_owned());
                    mset_vec.push(value.to_owned());
                } else {
                    let mut tmp = Vec::with_capacity(MSET_SIZE * 2);
                    std::mem::swap(&mut tmp, &mut mset_vec);
                    s.send(tmp);
                }

            }
            [redis::Value::Data(command), redis::Value::Data(key), redis::Value::Data(date)] if &*command == b"EXPIREAT"  => {

            }
            [ref v @ ..] => {
                //println!("Ignored command: {:?}", v)
            }
        }
    }

    s.send(mset_vec);

    });

    Ok(())
}