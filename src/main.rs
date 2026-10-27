mod meter;
mod ticket;
mod worker;

use std::thread;
use std::time::{Duration, Instant};
use ticket::{MemoryStore, Ticket, wait};
use worker::Worker;

/// Today's big value: 64 MiB.
const STOCK_BYTES: usize = 64 * 1024 * 1024;

/// A big result: 64 MiB of bytes, the pot of stock that many orders need.
fn make_stock() -> Vec<u8> {
    vec![7; STOCK_BYTES]
}

/// Where a value's bytes sit in memory. Equal addresses mean the very same
/// bytes, not two copies.
fn address(bytes: &[u8]) -> usize {
    bytes.as_ptr() as usize
}

fn main() {
    one_cook_copies_or_shares();
    two_cooks_one_pass();
}

/// One cook makes the stock; eight tasks need it.
fn one_cook_copies_or_shares() {
    let worker = Worker::start("worker-0");
    let stock = worker.submit(make_stock);
    let id = stock.id();
    let (made, _) = wait(vec![stock.clone()], 1, Duration::from_secs(60));
    println!(
        "submit make_stock         -> ticket {id}, ready: {}",
        made.len() == 1
    );

    let since = start_measuring();
    let copy = stock.get().expect("the stock never burns");
    let size = mib(copy.len());
    println!(
        "get {id}                    -> {size} MiB of bytes: {}",
        cost(since)
    );

    let since = start_measuring();
    let copies: Vec<Ticket<usize>> = (0..8)
        .map(|_| worker.submit_with(&stock, |bytes: Vec<u8>| address(&bytes)))
        .collect();
    let (copies, _) = wait(copies, 8, Duration::from_secs(60));
    let done = copies.len();
    println!("8 tasks, submit_with      -> {done} done: {}", cost(since));

    let since = start_measuring();
    let shared = stock.read().expect("the stock never burns");
    let size = mib(shared.len());
    println!(
        "read {id}                   -> {size} MiB of bytes: {}",
        cost(since)
    );

    let since = start_measuring();
    let readers: Vec<Ticket<usize>> = (0..8)
        .map(|_| worker.submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
        .collect();
    let (readers, _) = wait(readers, 8, Duration::from_secs(60));
    let done = readers.len();
    println!("8 tasks, submit_reading   -> {done} done: {}", cost(since));

    let home = address(&shared);
    let (copied, read) = (how_many_saw(&copies, home), how_many_saw(&readers, home));
    println!("saw {id}'s own bytes        -> copies: {copied} of 8, readers: {read} of 8");
}

/// How many of `tasks` reported the address `home`.
fn how_many_saw(tasks: &[Ticket<usize>], home: usize) -> usize {
    tasks
        .iter()
        .filter(|task| matches!(task.get(), Ok(seen) if seen == home))
        .count()
}

/// Two cooks share one pass: one numbering, and each reads the other's dishes.
fn two_cooks_one_pass() {
    let pass = MemoryStore::new();
    let cooks = [
        Worker::start_in("worker-1", &pass),
        Worker::start_in("worker-2", &pass),
    ];
    let quick = cooks[0].submit(|| 1_u64);
    println!("worker-1's quick dish     -> ticket {}", quick.id());
    let slow = cooks[1].submit(|| {
        thread::sleep(Duration::from_secs(1));
        2_u64
    });
    println!("worker-2's slow dish      -> ticket {}", slow.id());
    let clock = Instant::now();
    let (ready, _) = wait(vec![quick, slow], 2, Duration::from_secs(5));
    let after = round_to_100(clock.elapsed());
    println!(
        "wait for both, max 5 s    -> {} ready after about {after} ms",
        ready.len()
    );

    let stock = cooks[0].submit(make_stock);
    let id = stock.id();
    let home = address(&stock.read().expect("the stock never burns"));
    println!("worker-1 makes the stock  -> ticket {id}");
    let since = start_measuring();
    let reader = cooks[1].submit_reading(&stock, |bytes: &Vec<u8>| (cook_name(), address(bytes)));
    let (cook, seen) = reader.get().expect("reading never burns");
    let own = seen == home;
    println!(
        "{cook} reads {id}         -> {id}'s own bytes: {own}, {}",
        cost(since)
    );
}

/// The name of the thread running this code: inside a task, the cook's name.
fn cook_name() -> String {
    thread::current().name().unwrap_or("?").to_string()
}

/// Timings wobble by a millisecond or two; rounding keeps every run's output
/// identical.
fn round_to_100(elapsed: Duration) -> u128 {
    (elapsed.as_millis() + 50) / 100 * 100
}

/// Start measuring: the meter's count and the clock, both as of now.
fn start_measuring() -> (usize, Instant) {
    (meter::allocated(), Instant::now())
}

/// Time passed and fresh memory allocated since `start_measuring`.
fn cost(since: (usize, Instant)) -> String {
    let (bytes, clock) = since;
    let ms = clock.elapsed().as_secs_f64() * 1000.0;
    let fresh = meter::allocated() - bytes;
    format!("{ms:.1} ms, {:.1} MiB allocated", mib(fresh))
}

/// Bytes as mebibytes (1 MiB = 1024 × 1024 bytes).
fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}
