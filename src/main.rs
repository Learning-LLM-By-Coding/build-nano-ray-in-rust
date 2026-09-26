mod worker;

use std::thread;
use std::time::{Duration, Instant};
use worker::Worker;

/// A deliberately slow function — any plain Rust function works as a task.
fn slow_square(x: u64) -> u64 {
    thread::sleep(Duration::from_millis(300));
    x * x
}

fn main() {
    let worker = Worker::start("worker-0");

    let clock = Instant::now();
    let ticket = worker.submit(|| slow_square(7));
    let submitted = clock.elapsed().as_millis();
    println!("submitted slow_square(7); ticket in hand after {submitted} ms");

    let answer = ticket.get().expect("slow_square never fails");
    let waited = round_to_100(clock.elapsed());
    println!("get -> {answer} after about {waited} ms");

    let burnt = worker.submit(|| -> u64 { panic!("the oven caught fire") });
    match burnt.get() {
        Ok(value) => println!("unexpected success: {value}"),
        Err(error) => println!("get -> error: {error}"),
    }

    let next = worker
        .submit(|| slow_square(3))
        .get()
        .expect("the worker is still alive");
    println!("same worker, next task -> {next}");
}

/// Timings wobble by a millisecond or two; rounding keeps every run's output
/// identical.
fn round_to_100(elapsed: Duration) -> u128 {
    (elapsed.as_millis() + 50) / 100 * 100
}
