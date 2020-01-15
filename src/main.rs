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

const MSET_SIZE: usize = 400;

fn injector(i: u32, counter: &AtomicU64, addr: &str, r: crossbeam_channel::Receiver<Vec<(Vec<u8>,Vec<u8>,Vec<u8>)>>) -> Result<(), failure::Error> {
    let client = redis::Client::open(addr)?;
    let mut con = client.get_connection()?;

    let mut count = 0;
    while let Ok(bulk) = r.recv() {
        let len = bulk.len();
        let mut pipe = redis::pipe();
        let mut pipe = pipe.cmd("MSET");
        for (key, value, _) in bulk.iter() {
            pipe = pipe.arg(key.to_owned()).arg(value.to_owned());
        }
        pipe = pipe.ignore();
        for (key, _, expire_date) in bulk {
            pipe = pipe.cmd("EXPIREAT").arg(key).arg(expire_date).ignore();
        }
        pipe.query(&mut con)?;
        count += len;
        let cur = counter.fetch_add(len as u64, std::sync::atomic::Ordering::SeqCst);
        println!("thread {:>4} injected {:>8} entries - total: {:>8}", i, count, cur + len as u64);
    }

    Ok(())
}

fn main() -> Result<(), failure::Error> {

    let addr = std::env::args().nth(1).ok_or(failure::format_err!("missing redis addr"))?;
    let mut set_count = 0;
    let counter = AtomicU64::new(0);
    let mut hm = HashMap::new();
    crossbeam_utils::thread::scope(|scope| {

        let (s, r) = crossbeam_channel::bounded(1000  );

        for i in 0..16 {
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
    println!("There is {} keys remaining without an EXPIREAT", hm.len());
    let client = redis::Client::open(addr.as_str()).unwrap();
    let mut con = client.get_connection().unwrap();
    let mut pipe = redis::pipe();
    let mut pipe = &mut pipe;
    for (key, value) in hm {
        pipe = pipe.cmd("SET").arg(key).arg(value).ignore();
    }
    pipe.query::<()>(&mut con).unwrap();
    println!("Done injecting keys without EXPIREAT");

    println!("set count: {}", set_count);
    println!("total injected: {}", counter.load(std::sync::atomic::Ordering::SeqCst));

    Ok(())
}