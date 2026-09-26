mod worker;

use std::thread;
use worker::Worker;

fn main() {
    let worker = Worker::start("worker-0");
    println!(
        "main runs on thread {:?}",
        thread::current().name().unwrap_or("?")
    );

    for n in 1..=3 {
        worker.execute(Box::new(move || {
            let name = thread::current().name().unwrap_or("?").to_string();
            println!("job {n} runs on thread {name:?}");
        }));
    }

    // Dropping the worker waits for its queue to empty, then stops the thread.
    drop(worker);
    println!("all jobs done; worker stopped");
}
