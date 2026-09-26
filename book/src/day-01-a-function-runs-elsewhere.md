# Day 1: A function runs elsewhere

> **Day 1 of 17** · builds on `v1-day-00` · ≈ 1.5–2 h

| Section | Builds | Checkpoint |
|---------|--------|------------|
| A | A worker thread and its queue | `v1-day-01a` |
| B | Tickets for answers that do not exist yet | `v1-day-01b` |
| C | A crashed task is an answer too | `v1-day-01c` = `v1-day-01` |

**Pick your mode before starting** (explained fully in
[Setup](setup.md#4-choose-how-you-follow-along)):

```bash
# Build mode — type today's code yourself, on your own branch:
git switch -c my-course v1-day-00      # branch off the Day 0 scaffold

# Inspect mode — read along with the finished code instead:
git switch --detach v1-day-01a         # then 01b, 01c as you pass each section
```

<div class="callout">

**Today's typing budget** — so nothing sneaks up on you:

| Section | The learning | Wiring & tests (copy-friendly) |
|---------|--------------|--------------------------------|
| A | `Worker::start`, `execute`, `Drop` · ~35 lines | a demo `main`, 2 tests |
| B | `submit` + `Ticket` + `TaskError` · ~40 lines | a new `main`, 2 tests |
| C | catching the panic + `from_panic` · ~20 lines | 2 demo additions, 3 tests |

Three sections are three natural sittings, and every checkpoint is a save
point — nothing says a "day" must fit in one. Today needs **no
dependencies at all**: everything comes from Rust's standard library.

</div>

## 1. What you will have running

By the end of today, `cargo run` hands an ordinary Rust function to a
**worker** — a second line of work running alongside your program — and
gets a **ticket** back instantly, while the function is still running.
Later it trades the ticket in for the answer. Then it hands the same worker
a function that crashes, and gets back a clean error instead of a dead
program — and the worker carries straight on with the next job.

That is the first idea Ray is built on, in about 100 lines: **submit a
function, hold a ticket, collect the answer — or the reason there is
none.** Only one thing is small — there is exactly one worker, on this
machine. Everything else has the shape of the real thing.

## 2. What you will be seeing in the output

A sneak peek before anything else: here is the exact output you will
produce today, with a plain-English note under each line. Don't worry if a
note doesn't fully click yet — the rest of the day builds each piece:

```text
submitted slow_square(7); ticket in hand after 0 ms
get -> 49 after about 300 ms

thread 'worker-0' (136105154) panicked at src/main.rs:25:43:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get -> error: task failed: the oven caught fire
same worker, next task -> 9
```

- `submitted slow_square(7); ticket in hand after 0 ms` — `slow_square`
  takes 300 ms, yet the program got control back *immediately*. It did not
  wait: it handed the work away and kept a ticket.

- `get -> 49 after about 300 ms` — trading the ticket in for the answer
  waits only as long as the work actually takes. 7 × 7 = 49.

- The three `thread 'worker-0' … panicked` lines — a task crashed on
  purpose. This is Rust's standard crash report, printed by the worker
  thread (the number in brackets is a thread id and differs every run).

- `get -> error: task failed: the oven caught fire` — the crash did not
  take the program down. It came home through the ticket, as an ordinary
  error value, with its message intact.

- `same worker, next task -> 9` — and the worker survived: the very next
  job ran fine.

## 3. Why we need it

The Day 0 scaffold (`v1-day-00`) does everything on one thread, in one
place, in order. Every distributed system — Ray, Spark, a job queue behind
a website — starts from the opposite idea: **the code that asks for work
and the code that does the work are different workers**, and the asker
should not have to stand there waiting. Three primitives make that
possible:

- **a worker**: something that runs jobs somewhere else and keeps running,

- **a ticket**: a claim on a result that does not exist yet,

- **an error path**: a failure that travels back through the ticket
  instead of killing whoever did the work.

Today's three sections build one primitive each. And they are one story,
the one this whole book keeps telling: **the running story, for all 17
days, is a restaurant kitchen that grows into a chain.** Today it is a
single cook. Section A hires the cook, section B introduces order tickets,
section C decides what happens when a dish burns. (On Day 4 the kitchen
hires more cooks; on Day 7 it opens a second restaurant across town.) The
whole story, scene by scene, lives on one page —
[The restaurant chain: an analogy to help you build the right mental model](story.md) — which grows by one scene
every day.

## 4. Mental model

Walk the kitchen, one station per section:

- **The cook (section A's worker).** One cook, standing at the stove,
  working through order slips from a rail — one at a time, oldest first.
  The waiter (your `main`) never cooks; they pin a slip to the rail and
  walk away. At closing time the cook finishes every slip already on the
  rail before going home.

- **The order ticket (section B).** Pinning a slip gives the waiter a
  numbered ticket, on the spot — long before the food exists. The ticket
  is not the food; it is a *promise* of food. Showing the ticket at the
  pass — the counter where finished dishes come out of the kitchen — means
  "I'll wait here until that dish is ready."

- **The burnt dish (section C).** Sometimes a dish burns. A good kitchen
  doesn't close; the cook scrapes the pan, sends out a note — "sorry, the
  oven caught fire" — in place of the dish, and picks up the next slip.

```text
main (the waiter)                       worker-0 (the cook)
    │ submit(slow_square(7))
    │────────── job on the rail ───────────▶ starts cooking
    │◀───────── ticket, at once
    │ (free to do other things)              … 300 ms …
    │ ticket.get()  — waits at the pass
    │◀───────── Ok(49) ──────────────────── done
    │
    │ submit(burn it)
    │────────── job on the rail ───────────▶ panics!
    │ ticket.get()                           catches it
    │◀───────── Err("the oven caught fire")  keeps its apron on
```

## 5. Ray concepts introduced

- **Task**: a function sent to run somewhere else. In Ray you write
  `square.remote(7)`; today you write `worker.submit(|| square(7))`. Same
  idea, same promise: the call returns immediately.

- **Ticket (Ray's `ObjectRef`)**: what `.remote()` hands back — a
  reference to a result that may not exist yet. Today's `Ticket<T>` is the
  one-machine version; Day 2 gives tickets ids so they can be passed
  around and waited on in bulk.

- **`get`**: Ray's `ray.get(ref)` blocks until the result exists and
  returns it — or raises the task's error. Today's `Ticket::get` does
  exactly that, with Rust's `Result` in place of an exception.

- **Task errors**: when a Ray task raises, the exception is caught on the
  worker, shipped home, and re-raised at `ray.get` as a `RayTaskError`.
  The worker process stays alive for the next task. Today's `TaskError` is
  the same contract.

## 6. Build it, section by section

Each section opens with a **Rust used here** strip: the language concepts
its code leans on, linked to the matching pages of Google's
[Comprehensive Rust](https://google.github.io/comprehensive-rust/) course.
Click any of them to read that page in a side panel *beside* this chapter —
no prior Rust needed, and no leaving the page (Esc or × closes it).

### Section A — a worker thread and its queue

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/concurrency/threads/plain.html">threads</a>
<a href="https://google.github.io/comprehensive-rust/concurrency/channels/senders-receivers.html">channels</a>
<a href="https://google.github.io/comprehensive-rust/closures/syntax.html">closures</a>
<a href="https://google.github.io/comprehensive-rust/smart-pointers/trait-objects.html">trait objects (<code>Box&lt;dyn …&gt;</code>)</a>
<a href="https://google.github.io/comprehensive-rust/concurrency/send-sync/marker-traits.html"><code>Send</code></a>
<a href="https://google.github.io/comprehensive-rust/std-types/option.html"><code>Option</code></a>
<a href="https://google.github.io/comprehensive-rust/memory-management/drop.html"><code>Drop</code></a>
<a href="https://google.github.io/comprehensive-rust/testing/unit-tests.html">unit tests (<code>#[test]</code>, <code>#[cfg(test)]</code>)</a>
</div>

A **thread** is an independent line of execution inside your program: two
threads run at the same time, each with its own place in the code. Our
worker is one extra thread that loops: *take the next job, run it,
repeat* — until closing time. The rail of order slips is a **channel** — a queue with a sending
end and a receiving end. The sending end stays with `main`; the receiving
end moves into the worker thread.

What is a "job"? Any function the worker may call exactly once. In Rust
that is written `Box<dyn FnOnce() + Send + 'static>` — read it left to
right:

- `FnOnce()` — something callable with no arguments, at most once (a
  closure that captured `7` can be called; it cannot be un-called).

- `+ Send` — safe to move to another thread. The compiler checks this for
  every value you capture; share something that isn't thread-safe and the
  program does not compile.

- `+ 'static` — it owns everything it uses, so it cannot outlive a value
  it borrowed from `main`.

- `Box<dyn …>` — different closures have different sizes and types; a
  `Box` stores any of them behind one pointer, so a channel can carry all
  of them.

Create `src/worker.rs`:

```rust
# fn main() {}
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

/// A unit of work: any function the worker may call exactly once, safe to
/// move to another thread.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Analogy: one cook in the kitchen, working through order slips in order.
///
/// A background thread that runs jobs one at a time, in the order they
/// arrived.
pub struct Worker {
    sender: Option<Sender<Job>>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    /// Start a named worker thread, waiting for jobs.
    pub fn start(name: &str) -> Worker {
        let (sender, receiver) = mpsc::channel::<Job>();
        let handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                // The loop ends once every Sender is gone and the queue is empty.
                for job in receiver {
                    job();
                }
            })
            .expect("failed to spawn worker thread");
        Worker {
            sender: Some(sender),
            handle: Some(handle),
        }
    }

    /// Queue `job` for the worker and return immediately. Fire and forget.
    pub fn execute(&self, job: Job) {
        // A dead worker drops the job unrun; section B turns that into an error.
        let _ = self.sender.as_ref().expect("worker is running").send(job);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Closing the channel lets the worker finish its queue, then stop.
        drop(self.sender.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
```

Four things are doing all the work here:

- `mpsc::channel()` makes the rail. *mpsc* means "multiple producer,
  single consumer": many senders may pin slips, exactly one worker takes
  them. Receiving from a channel whose senders are all gone ends the
  `for job in receiver` loop — that is how the cook knows it's closing time.

- `thread::Builder::new().name(…).spawn(move || …)` starts the thread and
  names it `worker-0` — a name you will see in crash reports. `move`
  transfers the receiver into the thread: from now on, only the cook holds
  the receiving end.

- `execute` never waits. `send` puts the job on the rail and returns at
  once; whether the job has run yet is none of the caller's business.

- `Drop` is Rust's "closing time" hook: it runs automatically when a
  `Worker` goes out of scope. It drops the sender first (the rail closes),
  then `join`s the thread — *waits for it to finish*. So queued jobs are
  never lost on shutdown. The two `Option`s exist only so `Drop` can take
  each field out exactly once.

Now a demo. Replace `src/main.rs` with:

```rust,ignore
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
```

`cargo run`:

```text
main runs on thread "main"
job 1 runs on thread "worker-0"
job 2 runs on thread "worker-0"
job 3 runs on thread "worker-0"
all jobs done; worker stopped
```

Read it line by line:

- `main runs on thread "main"` — the waiter introduces itself. Your
  program's starting thread is always called `main`; `thread::current()`
  looked up its name.

- `job 1 runs on thread "worker-0"`, then jobs 2 and 3 — each order slip
  was cooked by the cook, not the waiter. `execute` put the three jobs on
  the rail, and the loop inside `Worker::start` ran them one by one, in the
  order they arrived.

- `all jobs done; worker stopped` — closing time. `drop(worker)` ran your
  `Drop`, which closed the rail and waited for the cook to finish every
  slip. That is why this line always prints last.

Pin both behaviors with tests. Append to `src/worker.rs`:

```rust,ignore
#[cfg(test)]
mod tests {
    use super::Worker;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn jobs_run_on_the_worker_thread_in_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let worker = Worker::start("worker-0");
        for n in 1..=3 {
            let seen = Arc::clone(&seen);
            worker.execute(Box::new(move || {
                let name = thread::current().name().unwrap_or("?").to_string();
                seen.lock().unwrap().push(format!("job {n} on {name}"));
            }));
        }
        drop(worker);
        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            [
                "job 1 on worker-0",
                "job 2 on worker-0",
                "job 3 on worker-0"
            ]
        );
    }

    #[test]
    fn dropping_the_worker_waits_for_queued_jobs() {
        let done = Arc::new(AtomicBool::new(false));
        let worker = Worker::start("worker-0");
        let flag = Arc::clone(&done);
        worker.execute(Box::new(move || {
            thread::sleep(Duration::from_millis(50));
            flag.store(true, Ordering::SeqCst);
        }));
        drop(worker);
        assert!(done.load(Ordering::SeqCst));
    }
}
```

Two new marks sit in that code. `#[test]` above a function tells Rust
"this is a test": `cargo test` finds and runs every function marked that
way. `#[cfg(test)]` above `mod tests` means "compile this module only when
running tests", so the tests never end up inside the program itself.
Lines that start with `#[` are called *attributes*: short notes to the
compiler, written just above the thing they describe.

The tests need somewhere for the worker to *write* while the test *reads*,
and two threads may not touch the same value unguarded — the compiler
refuses. Two standard tools make it legal, and you can read them as
labels for now: [`Arc`](https://google.github.io/comprehensive-rust/concurrency/shared-state/arc.html)
lets both threads hold the same value, and
[`Mutex`](https://google.github.io/comprehensive-rust/concurrency/shared-state/mutex.html)
(or an atomic, for a single flag) makes them take turns. Day 3's object
store gives shared data a proper home.

```bash
cargo test
```

```text
running 2 tests
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
```

(The Day 0 scaffold test is gone: it tested the `greeting` you just
replaced.)

**Section checkpoint:** `./scripts/compare.sh v1-day-01a` — its ✓ line
means your code matches the official section state (comments, blank lines,
and item order don't have to).

### Section B — tickets for answers that do not exist yet

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/generics/generic-functions.html">generic functions</a>
<a href="https://google.github.io/comprehensive-rust/generics/trait-bounds.html">trait bounds &amp; <code>where</code></a>
<a href="https://google.github.io/comprehensive-rust/closures/traits.html">closure traits (<code>FnOnce</code>)</a>
<a href="https://google.github.io/comprehensive-rust/memory-management/move.html">move semantics</a>
<a href="https://google.github.io/comprehensive-rust/error-handling/result.html"><code>Result</code></a>
<a href="https://google.github.io/comprehensive-rust/methods-and-traits/traits.html">implementing a trait (<code>Display</code>)</a>
<a href="https://google.github.io/comprehensive-rust/methods-and-traits/deriving.html">deriving a trait (<code>#[derive(Debug)]</code>)</a>
</div>

Your section A cook already runs anything you hand over — so why touch it?
Because `execute` is fire-and-forget: whatever a job returns, nobody ever
sees it. Real work has answers. The fix is the order ticket.

Here is the plan in plain words. When you submit a task, you make a
private channel with room for exactly one message: its result. The sending
end travels with the job to the worker. The receiving end comes back to
you, wrapped in a `Ticket`. When the task finishes, it sends its answer
down that private channel, and `get` waits at the other end.

Three edits to `src/worker.rs`, in order:

**First**, the imports. Replace the first line of the file with these two
lines (the second is the same channel import, now also naming `Receiver`):

```rust,ignore
use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
```

**Second**, the new `submit` method, inside `impl Worker`, right after the
closing brace of `execute`:

```rust,ignore
    // ── new code starts here (around line 43) ──
    /// Hand `task` to the worker and return a ticket for its result at once.
    pub fn submit<T, F>(&self, task: F) -> Ticket<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (result_sender, result_receiver) = mpsc::channel();
        self.execute(Box::new(move || {
            let value = task();
            // The caller may have thrown the ticket away; nobody to tell.
            let _ = result_sender.send(value);
        }));
        Ticket {
            receiver: result_receiver,
        }
    }
    // ── new code ends here ──
```

**Third**, the ticket and its error type, after the closing brace of
`impl Drop for Worker` and before `#[cfg(test)]`:

```rust,ignore
// ── new code starts here (around line 71) ──
/// Analogy: the numbered order ticket — proof you are owed a dish that may
/// not be cooked yet.
///
/// The receiving end of a private one-result channel; `get` blocks until the
/// task's result arrives.
pub struct Ticket<T> {
    receiver: Receiver<T>,
}

impl<T> Ticket<T> {
    /// Wait until the task finishes, then hand over its result.
    pub fn get(self) -> Result<T, TaskError> {
        // If the job was dropped unrun, its sender is gone and recv fails.
        self.receiver.recv().map_err(|_| TaskError {
            message: "worker stopped before the task finished".to_string(),
        })
    }
}

/// Why a task produced no result.
#[derive(Debug)]
pub struct TaskError {
    message: String,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task failed: {}", self.message)
    }
}
// ── new code ends here ──
```

Now read `submit`'s first line slowly. It is the most Rust-heavy line of
the day, and every part of it earns its place:

- `submit<T, F>` makes the method *generic*: one method that works for
  every task type `F` and every result type `T`. `slow_square` returns a
  `u64`; a later task might return a `String` or a `Vec`.

- The `where` clause states the rules. `F` must be callable once and
  return a `T`. Both must be safe to send to another thread (`Send`), and
  neither may borrow from the caller (`'static`). If a task breaks a rule,
  the program does not compile — you find out now, not from a race at 3 a.m.

- `submit` reuses section A instead of repeating it. It wraps your task in
  a job that also sends the answer home, then hands that job to `execute`.

Three details in the ticket are worth a second look:

- `#[derive(Debug)]` above `TaskError` asks the compiler to write the code
  that prints a `TaskError` for programmers, with `{:?}` — you get it for
  free instead of writing it yourself. That is different from `Display`,
  which you did write, and which is the polite message for users.
  `expect` uses the `Debug` form when it crashes, and you will see it in
  [Break it deliberately](#10-break-it-deliberately):
  `TaskError { message: "…" }`.

- `get(self)` takes the ticket *by value*, so cashing it in consumes it.
  Rust's move rules enforce the kitchen's rule for free: you cannot collect
  the same dish twice, because after `get` the ticket no longer exists.

- `recv()` fails in exactly one situation: every sender for this channel
  is gone without sending anything. That can only happen if the job never
  ran — for example, the worker thread had already died, so the job was
  thrown away. In that case `get` reports *"worker stopped before the task
  finished"* instead of waiting forever. Remember this line; you will watch
  it fire in [Break it deliberately](#10-break-it-deliberately).

Next, a real task to try it on. Replace `src/main.rs` with:

```rust,ignore
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
}

/// Timings wobble by a millisecond or two; rounding keeps every run's output
/// identical.
fn round_to_100(elapsed: Duration) -> u128 {
    (elapsed.as_millis() + 50) / 100 * 100
}
```

`cargo run`:

```text
submitted slow_square(7); ticket in hand after 0 ms
get -> 49 after about 300 ms
```

The first line comes from `submit`: the waiter had a ticket before the
cook had even started. The second comes from `get`: the waiter stood at
the counter for the 300 ms the cooking took, then got 49. That gap — 0 ms
to submit, 300 ms to collect — is the whole point of today. Between those
two lines, `main` was free to do anything else: submit ten more tasks,
read a file, answer a user. Nothing in the demo uses that freedom yet; on
Day 4, with several cooks, you will measure what it buys.

Two tests pin the new behavior. Two edits, inside `mod tests`:

1. Add `mpsc` to the tests' `std::sync` import (around line 106), so the
   line reads `use std::sync::{Arc, Mutex, mpsc};`.

2. Append these two tests after the closing brace of
   `dropping_the_worker_waits_for_queued_jobs`:

```rust,ignore
    // ── new code starts here (around line 145) ──
    #[test]
    fn get_returns_the_task_result() {
        let worker = Worker::start("worker-0");
        assert_eq!(worker.submit(|| 6 * 7).get().unwrap(), 42);
        let greeting = worker.submit(|| format!("hello from {}", "worker-0"));
        assert_eq!(greeting.get().unwrap(), "hello from worker-0");
    }

    #[test]
    fn submit_returns_before_the_task_finishes() {
        let worker = Worker::start("worker-0");
        let (open_gate, gate) = mpsc::channel::<()>();
        // The task cannot finish until we open the gate...
        let ticket = worker.submit(move || {
            gate.recv().unwrap();
            "cooked"
        });
        // ...yet we already hold its ticket. Now let it finish.
        open_gate.send(()).unwrap();
        assert_eq!(ticket.get().unwrap(), "cooked");
    }
    // ── new code ends here ──
```

The second test is worth a closer look. A timing test ("submit took under
1 ms") would fail now and then on a busy laptop. So instead, the task waits
on a *gate* — a channel that only the test can open. If `submit` waited for
the task to finish, the test would get stuck forever and never reach
`open_gate`. Holding the ticket while the task is provably unfinished is
the proof, and no clock is involved.

```text
running 4 tests
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

(The test lines may come out in a different order for you: the tests run
in parallel.)

**Section checkpoint:** `./scripts/compare.sh v1-day-01b`.

### Section C — a crashed task is an answer too

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/error-handling/panics.html">panics &amp; <code>catch_unwind</code></a>
<a href="https://google.github.io/comprehensive-rust/pattern-matching/match.html"><code>match</code></a>
<a href="https://google.github.io/comprehensive-rust/pattern-matching/let-control-flow/if-let.html"><code>if let</code></a>
<a href="https://google.github.io/comprehensive-rust/smart-pointers/box.html"><code>Box</code></a>
</div>

Your tickets work — for dishes that turn out fine. But what happens when a
task *panics*? A panic is Rust's word for a deliberate crash, like an
uncaught exception in other languages. Follow it step by step:

1. The panic unwinds the worker thread and kills it.

2. The task's result sender dies with the thread, so that task's `get`
   reports "worker stopped".

3. Every job still waiting on the rail is thrown away unrun, so every one
   of their tickets reports the same.

One burnt dish closes the whole kitchen. Ray refuses that trade: a failing
task is reported to the code that submitted it, and the worker carries on.
Rust has the tool for this. `std::panic::catch_unwind(f)` runs `f`; if `f`
panics, it stops the crash right there and returns `Err(payload)` instead,
where the *payload* is the panic's message, boxed up.

The plan: catch the panic where the task runs, and send the error home
through the same private channel as a normal answer. Five edits to
`src/worker.rs`, in order:

**First**, the imports. Add two lines at the top of the file, so it
starts:

```rust,ignore
use std::any::Any;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
```

**Second**, the ticket's channel now carries either an answer or an
error. In `pub struct Ticket<T>` (around line 81), change the field to:

```rust,ignore
    receiver: Receiver<Result<T, TaskError>>,
```

**Third**, `get` forwards whichever outcome arrived. Replace the body of
`get` (around line 86):

```rust,ignore
    pub fn get(self) -> Result<T, TaskError> {
        match self.receiver.recv() {
            Ok(outcome) => outcome,
            // The job was dropped unrun, so its sender is gone.
            Err(_) => Err(TaskError {
                message: "worker stopped before the task finished".to_string(),
            }),
        }
    }
```

**Fourth**, catch the panic where the task runs. In `submit` (around line
52), replace the `self.execute(…);` call with:

```rust,ignore
        self.execute(Box::new(move || {
            // A panic inside the task stops here instead of killing the worker.
            let outcome =
                panic::catch_unwind(AssertUnwindSafe(task)).map_err(TaskError::from_panic);
            // The caller may have thrown the ticket away; nobody to tell.
            let _ = result_sender.send(outcome);
        }));
```

**Fifth**, turn the panic's payload into a readable message. Add this
block after the closing brace of `pub struct TaskError`, before
`impl fmt::Display for TaskError`:

```rust,ignore
// ── new code starts here (around line 103) ──
impl TaskError {
    /// Turn a panic's payload into a readable message. `panic!("literal")`
    /// carries a `&str`; `panic!("{x}")` with formatting carries a `String`.
    fn from_panic(payload: Box<dyn Any + Send>) -> TaskError {
        let message = if let Some(text) = payload.downcast_ref::<&str>() {
            text.to_string()
        } else if let Some(text) = payload.downcast_ref::<String>() {
            text.clone()
        } else {
            "task panicked".to_string()
        };
        TaskError { message }
    }
}
// ── new code ends here ──
```

Two new names appeared in those edits:

- `AssertUnwindSafe` is you making a promise the compiler cannot check for
  itself: that a crash halfway through the task will not leave shared data
  half-updated. Our tasks share nothing with the worker, so the promise
  holds.

- In `from_panic`, a panic can carry a value of *any* type, so its payload
  arrives as `Box<dyn Any>` — "a boxed something, type unknown".
  `downcast_ref::<&str>()` asks "is it a `&str`?" and gives back `Some` if
  so. In practice the payload is one of the two string types, and the
  last `else` covers anything unusual.

Now give the kitchen a burnt dish. In `src/main.rs`, add these lines after
the `println!("get -> {answer} …")` line and before `main`'s closing
brace:

```rust,ignore
    // ── new code starts here (around line 24) ──
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
    // ── new code ends here ──
```

(The `-> u64` tells the compiler what the closure's result type would have
been. A closure that only panics never returns, so there is nothing to
infer it from.)

`cargo run`:

```text
submitted slow_square(7); ticket in hand after 0 ms
get -> 49 after about 300 ms
──── everything above: section B's output, unchanged ────

thread 'worker-0' (136105154) panicked at src/main.rs:25:43:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get -> error: task failed: the oven caught fire
same worker, next task -> 9
```

Read the new lines one by one:

- `thread 'worker-0' (…) panicked at src/main.rs:25:43:` and the two lines
  after it — the dish burns. This is Rust's standard crash report,
  printed by the cook's own thread the moment the panic happens, from the
  `panic!` in the task you just wrote. The number in brackets is a thread
  id and changes every run.

- `get -> error: task failed: the oven caught fire` — the bad news arrives
  at the counter as a note, not a closed kitchen. `catch_unwind` in
  `submit` caught the crash, `from_panic` turned it into a `TaskError`, and
  `get` handed it back; the `match` in `main` printed it through
  `TaskError`'s `Display`.

- `same worker, next task -> 9` — the cook is still on shift. The very
  next task, `slow_square(3)`, ran on the same `worker-0` and returned 9.

Three tests pin the new behavior. Append them inside `mod tests`, after
the closing brace of `submit_returns_before_the_task_finishes`:

```rust,ignore
    // ── new code starts here (around line 189) ──
    #[test]
    fn a_panicking_task_becomes_an_error_on_its_ticket() {
        let worker = Worker::start("worker-0");
        let ticket = worker.submit(|| -> u64 { panic!("the oven caught fire") });
        let error = ticket.get().unwrap_err();
        assert_eq!(error.to_string(), "task failed: the oven caught fire");
    }

    #[test]
    fn formatted_panic_messages_survive_too() {
        let worker = Worker::start("worker-0");
        let table = 7;
        let ticket = worker.submit(move || -> u64 { panic!("table {table} sent it back") });
        assert_eq!(
            ticket.get().unwrap_err().to_string(),
            "task failed: table 7 sent it back"
        );
    }

    #[test]
    fn the_worker_survives_a_panicking_task() {
        let worker = Worker::start("worker-0");
        let bad = worker.submit(|| -> u64 { panic!("boom") });
        assert!(bad.get().is_err());
        assert_eq!(worker.submit(|| 3 * 3).get().unwrap(), 9);
    }
    // ── new code ends here ──
```

**Section checkpoint:** `./scripts/compare.sh v1-day-01c` — which is also
the finished day.

## 7. Run it

```bash
cargo run
```

## 8. Expected result

```text
submitted slow_square(7); ticket in hand after 0 ms
get -> 49 after about 300 ms

thread 'worker-0' (136105154) panicked at src/main.rs:25:43:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get -> error: task failed: the oven caught fire
same worker, next task -> 9
```

The whole evening at the counter, one line at a time:

- `submitted slow_square(7); ticket in hand after 0 ms` — the waiter pins
  an order slip and walks away with a ticket at once. `submit` queued the
  task and returned without waiting.

- `get -> 49 after about 300 ms` — the waiter trades the ticket for the
  dish. `get` waited while the cook's `slow_square` slept for 300 ms, then
  handed over 7 × 7 = 49.

- The three `thread 'worker-0' … panicked` lines — a dish burns. The
  `panic!` in the task fired on the cook's thread, and Rust printed its
  standard crash report.

- `get -> error: task failed: the oven caught fire` — the note comes to
  the counter instead of the dish. `catch_unwind` caught the crash,
  `from_panic` wrote it up as a `TaskError`, and `get` delivered it.

- `same worker, next task -> 9` — the cook, still on shift, serves the next
  order.

Two things vary between runs. The thread id in brackets changes every
time, and the line number changes if your `main` is laid out differently.
Everything else is identical every run — including the order of the lines.
The crash report always lands before the `get -> error` line, because `get`
cannot return until the worker has finished handling the panic.

Notice the crash report still prints. `catch_unwind` stops the panic from
*killing* anything, but Rust's default panic hook still announces it — the
same way a Ray worker's traceback shows up in the logs even though your
driver program carries on.

## 9. Test it

Seven tests now pin today's behavior — execution on the worker thread,
order, and graceful shutdown (section A), tickets that arrive before the
answer and deliver it (section B), and panics turned into errors while the
worker survives (section C):

```bash
cargo test
```

```text
running 7 tests
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

(With `cargo test -- --nocapture` you would also see the crash reports of
the three panicking tasks; by default the test runner hides output from
passing tests.)

## 10. Break it deliberately

Remove the safety net. In `submit`, replace the `catch_unwind` line with a
plain call:

```diff
-            let outcome =
-                panic::catch_unwind(AssertUnwindSafe(task)).map_err(TaskError::from_panic);
+            let outcome = Ok(task());
```

`cargo run` now warns that `from_panic` and two imports are unused, and
then prints (thread ids and line numbers will differ):

```text
submitted slow_square(7); ticket in hand after 0 ms
get -> 49 after about 300 ms

thread 'worker-0' (136106654) panicked at src/main.rs:25:43:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get -> error: task failed: worker stopped before the task finished

thread 'main' (136106646) panicked at src/main.rs:34:10:
the worker is still alive: TaskError { message: "worker stopped before the task finished" }
```

Read it as a post-mortem, one line at a time:

- The first two lines are unchanged — the dish that cooks fine is not
  affected.

- The `thread 'worker-0' … panicked` lines — the burnt dish, exactly as
  before. But this time nothing catches the crash, so the cook's thread
  dies.

- `get -> error: task failed: worker stopped before the task finished` —
  the burnt task's result sender died with the thread, so `get` did not
  wait forever. Section B's safety line in `get` earned its keep. Notice
  what the message *lost*: it no longer says what went wrong, only that
  the worker is gone.

- `thread 'main' … panicked … the worker is still alive` — then `main`
  submitted `slow_square(3)`. The rail's receiving end died with the cook,
  so the job was thrown away unrun, and its ticket reported the same "worker
  stopped" error. The `expect` in `main` turned that error into a second
  crash — this time of the whole program.

The tests notice too. While the safety net is removed, `cargo test`
reports:

```text
test worker::tests::formatted_panic_messages_survive_too ... FAILED
test worker::tests::the_worker_survives_a_panicking_task ... FAILED
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... FAILED
```

(plus the four passing tests and a failure summary). All three section C
tests fail, and nothing else — each pins one piece of the protection you
removed.

Two lessons, both of which return with full force later:

- **A task's failure must stay the task's.** Without isolation, one bad
  input takes down the worker and every task queued behind it. Ray runs
  each worker as its own operating-system process, so a crash in one task
  cannot corrupt the program that submitted it.

- **"The worker died" is a different error from "the task failed."**
  Today's engine can at least *tell you* which one happened. Day 12 goes
  further: when a worker dies mid-task, the task is re-run elsewhere and
  the answer still arrives.

Restore the `catch_unwind` line and confirm `cargo test` is green again.

## 11. Checkpoint

Every checkpoint — each section and the day — is done only when the full
green bar passes:

```bash
./scripts/check.sh
```

Then close out the day in whichever mode you are following:

**Built along on your own branch?** Confirm your code matches the finished
day — the final ✓ line means it does:

```bash
./scripts/compare.sh v1-day-01
```

**Inspecting instead of typing?** Jump to today's exact finished state,
poke at it, and come back:

```bash
git switch --detach v1-day-01
cargo test
git switch -          # back to where you were
```

**Either way**, today's complete change set — docs included — reads as one
diff:

```bash
git diff v1-day-00 v1-day-01
```

## 12. Recap

**What changed today:** the project gained a `worker` module — one named
worker thread fed by a channel, with graceful shutdown on `Drop`;
`submit`, which turns any function into a task and returns a typed
`Ticket` at once; and panic isolation, so a crashing task becomes a
`TaskError` on its own ticket while the worker survives. Seven tests pin
it across three section checkpoints (`v1-day-01a`, `v1-day-01b`,
`v1-day-01c`), with no dependencies beyond the standard library.

**Deliberately naive:** there is one worker, so tasks run strictly one
after another; a ticket can only be cashed in by the code that holds it,
once, and cannot be handed to another task; results live inside the
ticket's private channel, so a big result is copied into it rather than
stored anywhere shared; and a task that loops forever blocks the kitchen
for good — there is no timeout.

**Tomorrow's limitation, resolved on Day 2:** today's ticket is a private
envelope. On Day 2 results move out of the envelopes into a table keyed by
id, and tickets become ids — so you can check without blocking, wait for
the first of several, and pass a ticket *into another task* before its
answer exists: the move that lets Ray chain work into pipelines.
