mod ticket;
mod worker;

use std::thread;
use std::time::{Duration, Instant};
use ticket::{Ticket, wait};
use worker::Worker;

/// A deliberately slow function — any plain Rust function works as a task.
fn slow_square(x: u64) -> u64 {
    thread::sleep(Duration::from_millis(300));
    x * x
}

/// Plain one-argument functions, ready to be fed another task's result.
/// They print so you can see when, and whether, the cook runs them.
fn add_one(x: u64) -> u64 {
    println!("    cook: add_one({x}) = {}", x + 1);
    x + 1
}

fn double(x: u64) -> u64 {
    println!("    cook: double({x}) = {}", x * 2);
    x * 2
}

fn times_ten(x: u64) -> u64 {
    println!("    cook: times_ten({x}) = {}", x * 10);
    x * 10
}

/// A slow task that always fails: the dish burns after 300 ms on the stove.
fn slow_burn() -> u64 {
    thread::sleep(Duration::from_millis(300));
    panic!("the oven caught fire")
}

fn main() {
    let worker = Worker::start("worker-0");

    let ticket = worker.submit(|| slow_square(7));
    println!("submit slow_square(7) -> ticket {}", ticket.id());

    let copy = ticket.clone();
    println!("clone the ticket      -> ticket {}", copy.id());

    let answer = ticket.get().expect("slow_square never fails");
    println!("get ticket {}         -> {answer}", ticket.id());

    let again = copy.get().expect("the dish is still on the pass");
    println!("get the copy          -> {again}");

    let burnt = worker.submit(slow_burn);
    println!("submit a burning task -> ticket {}", burnt.id());

    for attempt in 1..=2 {
        match burnt.get() {
            Ok(value) => println!("unexpected success: {value}"),
            Err(error) => println!(
                "get ticket {} (try {attempt}) -> error: {error}",
                burnt.id()
            ),
        }
    }

    let orders: Vec<Ticket<u64>> = (1..=3)
        .map(|x| worker.submit(move || slow_square(x)))
        .collect();
    println!("submit 3 orders       -> tickets {}", numbers(&orders));
    let clock = Instant::now();

    let (ready, pending) = wait(orders, 1, Duration::from_secs(5));
    show_wait("wait for 1, max 5 s  ", &ready, &pending, clock);

    let (ready, pending) = wait(pending, 1, Duration::from_millis(100));
    show_wait("wait for 1, max 0.1 s", &ready, &pending, clock);

    let (ready, pending) = wait(pending, 2, Duration::from_secs(5));
    show_wait("wait for 2, max 5 s  ", &ready, &pending, clock);

    let (ready, _) = wait(vec![burnt], 1, Duration::ZERO);
    println!(
        "wait on the burnt one -> ready {} (a failure counts as done)",
        numbers(&ready)
    );

    let base = worker.submit(|| slow_square(3));
    println!("submit slow_square(3) -> ticket {}", base.id());
    let plus_one = worker.submit_with(&base, add_one);
    println!(
        "submit add_one({})    -> ticket {}",
        base.id(),
        plus_one.id()
    );
    let doubled = worker.submit_with(&plus_one, double);
    println!(
        "submit double({})     -> ticket {}",
        plus_one.id(),
        doubled.id()
    );
    let tenfold = worker.submit_with(&base, times_ten);
    println!(
        "submit times_ten({})  -> ticket {}",
        base.id(),
        tenfold.id()
    );
    let (ready, _) = wait(vec![base.clone()], 1, Duration::ZERO);
    println!("is {} ready yet?      -> {}", base.id(), !ready.is_empty());

    let answer = doubled.get().expect("nothing burns here");
    println!("get ticket {}         -> {answer}", doubled.id());
    let answer = tenfold.get().expect("nothing burns here");
    println!("get ticket {}         -> {answer}", tenfold.id());

    let ruined = worker.submit(slow_burn);
    println!("submit slow_burn      -> ticket {}", ruined.id());
    let rescue = worker.submit_with(&ruined, add_one);
    println!(
        "submit add_one({})   -> ticket {}",
        ruined.id(),
        rescue.id()
    );
    match rescue.get() {
        Ok(value) => println!("unexpected success: {value}"),
        Err(error) => println!("get ticket {}        -> error: {error}", rescue.id()),
    }
}

/// Print one `wait` step: which tickets are ready, and when that was.
fn show_wait<T>(label: &str, ready: &[Ticket<T>], pending: &[Ticket<T>], clock: Instant) {
    let at = round_to_100(clock.elapsed());
    let (ready, pending) = (numbers(ready), numbers(pending));
    println!("{label} -> ready {ready}, pending {pending} at about {at} ms");
}

/// Ticket numbers as a list, like `[#3, #4]`.
fn numbers<T>(tickets: &[Ticket<T>]) -> String {
    let ids: Vec<String> = tickets
        .iter()
        .map(|ticket| ticket.id().to_string())
        .collect();
    format!("[{}]", ids.join(", "))
}

/// Timings wobble by a millisecond or two; rounding keeps every run's output
/// identical.
fn round_to_100(elapsed: Duration) -> u128 {
    (elapsed.as_millis() + 50) / 100 * 100
}
