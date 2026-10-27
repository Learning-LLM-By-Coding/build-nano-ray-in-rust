mod ticket;
mod worker;

use std::thread;
use std::time::Duration;
use worker::Worker;

/// A deliberately slow function — any plain Rust function works as a task.
fn slow_square(x: u64) -> u64 {
    thread::sleep(Duration::from_millis(300));
    x * x
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
}
