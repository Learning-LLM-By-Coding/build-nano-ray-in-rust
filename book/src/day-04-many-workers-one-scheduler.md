# Day 4: Many workers, one scheduler

> **Day 4 of 17** · builds on `v1-day-03` · ≈ 2 h

| Section | Builds | Checkpoint |
|---------|--------|------------|
| A | One rail for the whole crew: a worker pool | `v1-day-04a` |
| B | Anyone who takes slips: one `Submit` for every kind of worker | `v1-day-04b` |
| C | More cooks, less time: the speedup, measured | `v1-day-04c` = `v1-day-04` |

**Pick your mode before starting** (explained fully in
[Setup](setup.md#4-choose-how-you-follow-along)):

```bash
# Build mode — start (or restart) your branch from yesterday's checkpoint.
# Already on my-course from Day 3? Keep going — no command needed.
git switch -c my-course v1-day-03

# Inspect mode — read along with the finished code instead:
git switch --detach v1-day-04a         # then 04b, 04c as you pass each section
```

<div class="callout">

**Today's typing budget** — so nothing sneaks up on you:

| Section | The learning | Wiring & tests (copy-friendly) |
|---------|--------------|--------------------------------|
| A | `Pool`: `start`, `take_slips`, `Drop` · ~60 lines in a new `src/pool.rs` | four `pub(crate)` marks, two demo functions + a `report` helper, 4 tests |
| B | the `Submit` trait · ~25 new lines; four methods you already have *move* into it | imports in four files, one demo function, 4 tests |
| C | `is_prime`, `count_primes`, `share`, `count_on` · ~45 lines | two timing helpers + one demo function, 3 tests |

Section A is the longest sitting of the day. Section B is mostly moving
code you already wrote; section C is mostly measuring. Three sections are
three natural sittings, and every checkpoint is a save point — nothing
says a "day" must fit in one. Still **no dependencies**: everything comes
from Rust's standard library.

</div>

## 1. What you will have running

Since Day 3, several workers can share one store. But *you* still choose
which worker gets each job, at the moment you hand it out — before anyone
knows which worker will be free first. By the end of today, `cargo run`
does four new things:

- **shows the cost of choosing by hand.** Four jobs, one of them long,
  split between two workers in turn: one worker falls behind while the
  other sits idle for most of the time.

- **stops choosing.** A *pool* of workers shares a single queue of jobs.
  Whichever worker is free takes the next job, and the same four jobs
  finish sooner.

- **keeps everything from Days 2 and 3 working through the pool:** a job
  that waits for another job's result, and many jobs reading one big
  stored value without copying it.

- **measures the payoff on real work.** One slow, purely computational
  job, cut into pieces, runs on 1, 2, and 4 workers — on this machine it
  drops to a bit more than a quarter of its time — and you find out why it
  is not *exactly* a quarter.

That is Ray's worker pool and scheduler in miniature: **you say what to
run, the runtime decides where.**

<div class="dl-concepts"><span>Ray concepts used here:</span>
<a href="https://docs.ray.io/en/latest/ray-core/key-concepts.html">key concepts: tasks and workers ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/scheduling/index.html">scheduling ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/scheduling/resources.html">resources: one CPU per task by default ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/patterns/too-fine-grained-tasks.html">anti-pattern: too fine-grained tasks harm speedup ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/tips-for-first-time.html#tip-2-avoid-tiny-tasks">tip: avoid tiny tasks ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/patterns/ray-get-loop.html">anti-pattern: ray.get in a loop harms parallelism ↗</a>
<a href="https://en.wikipedia.org/wiki/Amdahl%27s_law">Amdahl's law (Wikipedia) ↗</a>
</div>

## 2. What you will be seeing in the output

A sneak peek at the end of the day — the exact output you will produce,
with a plain-English note under each group of lines. Day 3's sixteen
lines still print first; today's lines follow them. If a note doesn't
fully land yet, that's fine; the rest of the day builds every piece:

```text
⋮ (Day 3's sixteen lines print first, unchanged)
picked by hand, 2 cooks   -> all 4 slips done after about 1000 ms
  the big slip's cook     -> 2 of 4 slips, done at about 1000 ms, then idle 0 ms
  the other cook          -> 2 of 4 slips, done at about 400 ms, then idle 600 ms
one rail, a crew of 2     -> all 4 slips done after about 800 ms
  the big slip's cook     -> 1 of 4 slips, done at about 800 ms, then idle 0 ms
  the other cook          -> 3 of 4 slips, done at about 600 ms, then idle 200 ms
sauce cooking, pasta held -> 3 quick slips done after about 0 ms
pasta with sauce          -> ready after about 300 ms
8 readers on a crew of 4  -> 8 done: 120.1 ms, 0.0 MiB allocated
cooks used, bytes seen    -> 4 of 4 cooks, the stock's own bytes: 8 of 8
this machine              -> 18 cores
the job                   -> primes up to 6000000, as 16 slips
a crew of 1               -> 412849 primes in 2088 ms, 1.00x as fast as 1 cook
a crew of 2               -> 412849 primes in 1104 ms, 1.89x as fast as 1 cook
a crew of 4               -> 412849 primes in 599 ms, 3.49x as fast as 1 cook
slip 1 alone              -> 31904 primes in 45 ms
slip 16 alone             -> 24057 primes in 178 ms
a crew of 4, 64 slips     -> 412849 primes in 571 ms, 3.65x as fast as 1 cook
```

- `picked by hand, 2 cooks` and the two lines under it — four jobs: one
  takes 800 ms, three take 200 ms each. They are handed to two workers in
  turn, so the first worker gets the long job *and* a short one: 1000 ms
  of work. The second worker is done after 400 ms, then waits, idle, for
  600 ms.

- `one rail, a crew of 2` and the two lines under it — the same four jobs
  on a pool of two workers sharing one queue. One worker takes the long
  job; the other takes the three short ones, one after another. All four
  are done after 800 ms instead of 1000 — and 800 ms is the long job
  alone, so nothing could finish sooner.

- `sauce cooking, pasta held` and `pasta with sauce` — a pool of four.
  One job takes 300 ms and a second job needs its result. The waiting job
  holds up no worker: three quick jobs finish at once. The waiting job
  runs the moment its input is ready, at about 300 ms.

- `8 readers on a crew of 4` and `cooks used` — eight jobs read one stored
  64 MiB value through the pool: no fresh memory, all four workers took a
  share, and every job saw the stored value's own bytes.

- `this machine` down to `a crew of 4` — the measurement. The computer has
  18 cores. One job — count the prime numbers up to six million — is cut
  into 16 pieces and run on 1, 2, and 4 workers. Two workers take about
  half the time; four take a bit more than a quarter.

- `slip 1 alone`, `slip 16 alone` — why not exactly a quarter: the pieces
  are not equal. The last piece holds the biggest numbers and takes about
  four times as long as the first.

- `a crew of 4, 64 slips` — cut the job into 64 smaller pieces instead,
  and four workers get closer to four times as fast.

## 3. Why we need it

**The story so far — remember [the restaurant](story.md):**
[cooks](story.md#worker) work through [order slips](story.md#task) pinned
to their rails. Finished dishes land on
[one pass for the whole kitchen](story.md#shared-pass), every pot sits
once in [the pantry](story.md#pantry), and the waiter can
[stock the pantry](story.md#put) directly.

Today's scene: **the crew.** The kitchen has more than one cook now, and
three things about the way they get their work start to hurt:

- **The waiter picks the cook.** Every slip goes to a cook the waiter
  chose — on Day 3 with `cooks[n % 2]`, taking turns. The waiter has to
  decide when the slip is written, long before anyone knows how long each
  dish will take.

- **A line forms behind a busy cook.** Each cook has a rail of their own.
  A slip pinned behind a big slip waits there, even while the cook at the
  next stove has nothing to do. You will measure exactly how long.

- **Nobody has measured whether more cooks help.** Two cooks *ought* to
  cook twice as fast. On a real computer, does a job really drop to half
  its time — and with four cooks, to a quarter?

Section A gives the crew one shared rail, so the waiter never picks a
cook again. Section B makes everything you built on Days 2 and 3 work on
the crew. Section C measures the speedup on real work, honestly.

## 4. Mental model

Walk the kitchen, one station per section:

- **One rail for the whole crew (section A).** The cooks stop having
  rails of their own. The waiter pins every slip to *one* rail, in order.
  A cook who is free walks to the rail, takes the first slip, cooks it,
  and comes back for the next. Nobody assigns anything: the cook who is
  free first gets the next slip. That simple rule is today's
  **scheduler**.

- **Anyone who takes slips (section B).** The waiter hands a crew exactly
  the same slips as a lone cook: plain slips, [held slips](story.md#held-slip)
  clipped to the pass, slips that read a pot [in place](story.md#read-in-place).
  A held slip still waits on the pass, not on the rail. It takes no cook
  while it waits, and once its input lands it joins the one rail like any
  other slip.

- **More cooks, less time — measured (section C).** A big job is cut into
  slips, and the crew works through them side by side. The job is done
  when the *last* slip is done. So the crew is only as fast as its
  slowest finish — and that is why the size of the slips matters.

```text
the waiter pins slips               every slip goes on the one shared rail, in order
   │ the rail: first come, first served
   ▼
whichever cook is free              takes the first slip; the other cooks keep cooking
   │ the cook cooks it
   ▼
the pass                            the dish lands under its ticket number
   │ the cook walks back to the rail
   ▼
back to the top: take the next slip — until the rail is empty and closed
```

Read it top to bottom, as one cook's evening: the left column is where
the slip is, the right column says what happens there. Every cook in the
crew walks the same loop at the same time, and the only place they meet
is the rail.

## 5. Ray concepts introduced

- **The worker pool.** Each Ray node starts a set of worker processes and
  keeps them running, ready for tasks. By default every task needs one
  CPU, and a node offers one CPU per core, so a node runs about as many
  tasks at once as it has cores. Today's `Pool` is that set of workers:
  a fixed number of threads, started once and reused for every task.

- **The scheduler.** In Ray, you call `f.remote()` and never name a
  worker; the scheduler on each node hands every waiting task to a free
  worker. Today's scheduler is one rule — *whoever is free takes the next
  task* — built from one shared queue. Ray's scheduler *pushes*: it picks
  a free worker and hands it the task. Ours *pulls*: free workers take
  tasks off the queue. On one machine the effect is the same: no task waits
  while a worker sits idle. Day 11 teaches the scheduler to prefer the
  machine where a task's inputs already are, and Day 16 to count CPU and
  GPU slots.

- **One call, wherever it runs.** `f.remote()` looks the same whether one
  worker or a thousand stand behind it. Today's `Submit` trait gives you
  the same: `submit`, `submit_reading`, and `submit_with` are written
  once, and work on a lone worker or on a whole pool.

- **Task granularity.** Ray's tips for first-time users warn against tiny
  tasks: each task carries a fixed overhead, so too many tiny tasks spend
  their time on bookkeeping. Today you meet the other edge: too *few*
  tasks of unequal size leave workers idle while the last big one
  finishes. The right size sits between the two.

- **Speedup, and Amdahl's law.** *Speedup* is the time on one worker
  divided by the time on N workers. It reaches N only if every part of the
  job can be split and the parts are equal. Amdahl's law names the other
  limit: the part that cannot be split — handing out tasks, collecting
  answers — caps the speedup however many workers you add. Today that
  part takes well under a millisecond, so it does not show yet. It starts
  to matter on Day 7, when every task crosses a network.

- **One honest difference.** Ray's workers are separate processes, and a
  task's arguments reach them through the object store. Ours are threads
  in one program, so a task reaches a worker as a plain Rust closure. On
  Day 7 workers move to other machines, and that shortcut ends.

## 6. Build it, section by section

### Section A — one rail for the whole crew

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.enumerate"><code>enumerate</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/fmt/index.html#width">format width (<code>{label:&lt;26}</code>, std docs)</a>
<a href="https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.partition"><code>partition</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/option/enum.Option.html#method.unwrap_or_default"><code>unwrap_or_default</code> (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/control-flow-basics/loops/loop.html"><code>loop</code></a>
<a href="https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.filter_map"><code>filter_map</code> (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/modules/visibility.html">visibility (<code>pub(crate)</code>)</a>
<a href="https://doc.rust-lang.org/std/vec/struct.Vec.html#method.drain"><code>Vec::drain</code> (std docs)</a>
</div>

Your Day 3 kitchen already has two cooks on one pass, and `cooks[n % 2]`
handed them slips in turn. That works — so why change it? Measure it
first. Tonight brings four slips: one big slip that takes 800 ms, then
three quick ones of 200 ms each. Each slip reports which cook cooked it,
and when it was done.

Two edits to `src/main.rs`:

1. After the closing brace of `the_waiter_stocks_the_pantry` (around line
   162), add the four slips, what each slip does, the scene, and a helper
   that prints the result:

   ```rust,ignore
   // ──────── new code starts here (around line 163) ────────
   /// Tonight's four slips: one big one (800 ms of cooking), then three quick
   /// ones (200 ms each).
   const SLIPS_MS: [u64; 4] = [800, 200, 200, 200];

   /// Cook for `ms` milliseconds, then report who cooked and when, on `clock`,
   /// the slip was done.
   fn cook_for(ms: u64, clock: Instant) -> (String, Duration) {
       thread::sleep(Duration::from_millis(ms));
       (cook_name(), clock.elapsed())
   }

   /// Two cooks, each with a rail of their own; the waiter picks a cook for
   /// every slip, in turn.
   fn picked_by_hand() {
       let pass = MemoryStore::new();
       let cooks = [
           Worker::start_in("worker-1", &pass),
           Worker::start_in("worker-2", &pass),
       ];
       let clock = Instant::now();
       let slips: Vec<Ticket<(String, Duration)>> = SLIPS_MS
           .iter()
           .enumerate()
           .map(|(n, &ms)| cooks[n % 2].submit(move || cook_for(ms, clock)))
           .collect();
       report("picked by hand, 2 cooks", &slips);
   }

   /// When all four slips were done, then each cook's share of them: first the
   /// cook who got the big slip, then the other one.
   fn report(label: &str, slips: &[Ticket<(String, Duration)>]) {
       let done: Vec<(String, Duration)> = slips
           .iter()
           .map(|slip| slip.get().expect("no slip burns"))
           .collect();
       let whole = done.iter().map(|(_, at)| *at).max().expect("4 slips");
       println!(
           "{label:<26}-> all 4 slips done after about {} ms",
           round_to_100(whole)
       );
       // Split the slips in two: those the big slip's cook cooked, and the rest.
       let (big, other): (Vec<_>, Vec<_>) = done.iter().partition(|(cook, _)| *cook == done[0].0);
       for (who, theirs) in [("the big slip's cook", big), ("the other cook", other)] {
           let last = theirs.iter().map(|(_, at)| *at).max().unwrap_or_default();
           println!(
               "  {who:<24}-> {} of 4 slips, done at about {} ms, then idle {} ms",
               theirs.len(),
               round_to_100(last),
               round_to_100(whole - last)
           );
       }
   }
   // ──────── new code ends here ────────
   ```

2. Add `picked_by_hand();` as the last line of `main`.

A few details:

- `SLIPS_MS` is a `const` array: four numbers, fixed when the program is
  compiled. `[u64; 4]` reads "an array of four `u64`s".

- `cook_for` is what every slip does: it sleeps for `ms` milliseconds — a cook
  busy at the stove — then returns the cook's name and the time on
  `clock`, a stopwatch the waiter started before handing out the first
  slip. `Instant` is a small `Copy` value, so every slip's `move` closure
  gets its own copy of the same starting moment.

- `.enumerate()` pairs each item with its position: `(0, &800)`,
  `(1, &200)`, …. Slip `n` goes to `cooks[n % 2]`: slips 0 and 2 to
  `worker-1`, slips 1 and 3 to `worker-2`.

- `report` waits for all four slips, then prints two things: when *all*
  four were done (the latest finish), and each cook's share.
  `{label:<26}` prints `label` padded with spaces to 26 characters, so the
  `->` arrows line up.

- `partition` splits the results into two lists in one pass: those whose
  test is `true` (cooked by the same cook as slip 0, the big one), and the
  rest. The `for` loop then prints one line per list. `unwrap_or_default`
  turns "this cook cooked nothing" into a time of zero instead of a
  crash.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 3.6 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 11.7 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #2
wait for both, max 5 s    -> 2 ready after about 1000 ms
worker-1 makes the stock  -> ticket #3
worker-2 reads #3         -> #3's own bytes: true, 0.0 ms, 0.0 MiB allocated
both cooks busy, 300 ms   -> tickets #1 and #2
put the delivery          -> ticket #3, ready: true, 0.0 ms, 0.0 MiB allocated
cooks finished yet?       -> 0 of 2
8 tasks on 2 cooks        -> 8 done: 306.3 ms, 0.0 MiB allocated
saw the delivery's bytes  -> 8 of 8
──── everything above: Day 3's output, unchanged ────
picked by hand, 2 cooks   -> all 4 slips done after about 1000 ms
  the big slip's cook     -> 2 of 4 slips, done at about 1000 ms, then idle 0 ms
  the other cook          -> 2 of 4 slips, done at about 400 ms, then idle 600 ms
```

Read the new lines one by one:

- `picked by hand, 2 cooks -> all 4 slips done after about 1000 ms` —
  the four slips took a full second. `report` found the latest finish
  among them.

- `the big slip's cook -> 2 of 4 slips, done at about 1000 ms` —
  `worker-1` got slip 0 (800 ms) *and* slip 2 (200 ms), so its rail held
  1000 ms of cooking. Slip 2 waited 800 ms behind the big slip.

- `the other cook -> 2 of 4 slips, done at about 400 ms, then idle 600 ms`
  — `worker-2` cooked its two quick slips and then stood idle for 600 ms,
  while slip 2 sat on the other cook's rail.

Could the waiter pick more cleverly? Only by knowing in advance how long
every slip will take — and the waiter never does. The fix is to stop
picking at all: put every slip on **one** rail, and let each cook take
the next slip the moment they are free.

Day 1's rail was a channel: one `Sender` the waiter pins slips with, one
`Receiver` the cook takes them from. A channel allows many `Sender`s but
only one `Receiver`. So the crew shares the one `Receiver`: it goes into
a `Mutex`, so only one cook at a time can take a slip, and the `Mutex`
goes into an `Arc`, so every cook holds a handle on it.

First, the pool needs two of the worker's private tools:
[the cook's stub](story.md#stub) (`Promise`) and `run_catching`. Make
them visible to the rest of your crate. Four one-word edits to `src/worker.rs`, each adding `pub(crate)`
at the start of a line:

1. `fn run_catching<T, F>(task: F) -> Outcome` (around line 123) becomes
   `pub(crate) fn run_catching<T, F>(task: F) -> Outcome`.

2. `struct Promise {` (around line 139) becomes `pub(crate) struct
   Promise {`.

3. Inside `impl Promise`, `fn new(` (around line 146) becomes
   `pub(crate) fn new(`.

4. Inside `impl Promise`, `fn keep(` (around line 155) becomes
   `pub(crate) fn keep(`.

`pub(crate)` means "public inside this program, invisible to anything
outside it" — enough for a new file to use them. These marks are
temporary: in section B the code that needs them moves back into
`worker.rs`, and you will remove them again.

Now create `src/pool.rs`:

```rust,ignore
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::ticket::{MemoryStore, Ticket};
use crate::worker::{Job, Promise, run_catching};

/// Analogy: a crew of cooks working from one shared rail — whichever cook is
/// free takes the next slip.
///
/// A fixed number of worker threads behind one job queue. Every thread takes
/// the next job the moment it is free, so no job waits behind a busy thread
/// while another thread sits idle.
pub struct Pool {
    sender: Option<Sender<Job>>,
    handles: Vec<JoinHandle<()>>,
    store: Arc<MemoryStore>,
}

impl Pool {
    /// Start `size` worker threads, named `worker-1` to `worker-<size>`, that
    /// share one queue and put their results in `store`.
    pub fn start(size: usize, store: &Arc<MemoryStore>) -> Pool {
        let (sender, receiver) = mpsc::channel::<Job>();
        let rail = Arc::new(Mutex::new(receiver));
        let handles = (1..=size)
            .map(|n| {
                let rail = Arc::clone(&rail);
                thread::Builder::new()
                    .name(format!("worker-{n}"))
                    .spawn(move || take_slips(&rail))
                    .expect("failed to spawn worker thread")
            })
            .collect();
        Pool {
            sender: Some(sender),
            handles,
            store: Arc::clone(store),
        }
    }

    /// Queue `job` for whichever worker is free next, and return immediately.
    pub fn execute(&self, job: Job) {
        // If the pool is gone the job is dropped unrun (its Promise reports it).
        let _ = self.sender.as_ref().expect("pool is running").send(job);
    }

    /// Hand `task` to the pool and return a ticket for its result at once.
    pub fn submit<T, F>(&self, task: F) -> Ticket<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let id = self.store.new_id();
        let promise = Promise::new(id, Arc::clone(&self.store));
        self.execute(Box::new(move || promise.keep(run_catching(task))));
        Ticket::new(id, Arc::clone(&self.store))
    }
}

/// Analogy: one cook's evening — take the next slip from the shared rail,
/// cook it, come back for another, until the rail is empty and closed.
///
/// The loop every pool thread runs.
fn take_slips(rail: &Mutex<Receiver<Job>>) {
    loop {
        // The lock is held only while taking a job, never while running it.
        let next = rail.lock().unwrap().recv();
        match next {
            Ok(job) => job(),
            // Every Sender is gone and the queue is empty: the evening is over.
            Err(_) => return,
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Closing the channel lets the workers finish the queue, then stop.
        drop(self.sender.take());
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}
```

Read it piece by piece:

- `Pool` has the same three parts as `Worker`, with one change: a list of
  thread handles (`handles`) instead of one.

- `Pool::start` makes one channel, wraps its `Receiver` as
  `Arc<Mutex<Receiver<Job>>>` — the rail — and starts `size` threads,
  named `worker-1`, `worker-2`, and so on. Each thread gets its own
  `Arc::clone` of the rail.

- `execute` and `submit` are `Worker`'s, word for word, except for the
  message `"pool is running"`. The slip goes on the shared rail, and
  whichever cook takes it fills the stub.

- `take_slips` is one cook's evening: a `loop` runs its body again and
  again until a `return` ends it. `rail.lock().unwrap().recv()` locks
  the rail, waits for the next slip, and takes it; the `match` then cooks
  the slip (`Ok(job)`) or ends the evening (`Err(_)`). The lock lives only
  until the end of that `let` statement — so the cook lets go of the rail
  *before* cooking, and the next cook can take a slip meanwhile. When
  every `Sender` is gone and the rail is empty, `recv` returns an error
  and the cook goes home.

- `Drop` closes the rail by dropping the `Sender`, then waits for every
  cook to finish the slips still on it. `self.handles.drain(..)` takes the
  handles out of the list one by one, so each can be joined.

Last, the scene. Four edits to `src/main.rs`:

1. Add `mod pool;` between `mod meter;` and `mod ticket;` (line 2).

2. Add `use pool::Pool;` above `use std::thread;` (line 6).

3. After the closing brace of `report` (around line 218), add the same
   four slips, now on a crew of two:

   ```rust,ignore
   // ──────── new code starts here (around line 219) ────────
   /// The same four slips on a crew of two cooks sharing one rail.
   fn one_rail_for_the_crew() {
       let pass = MemoryStore::new();
       let crew = Pool::start(2, &pass);
       let clock = Instant::now();
       let slips: Vec<Ticket<(String, Duration)>> = SLIPS_MS
           .iter()
           .map(|&ms| crew.submit(move || cook_for(ms, clock)))
           .collect();
       report("one rail, a crew of 2", &slips);
   }
   // ──────── new code ends here ────────
   ```

4. Add `one_rail_for_the_crew();` as the last line of `main`.

The only differences from `picked_by_hand`: one crew instead of two cooks,
and no `enumerate` — the waiter never chooses, so the slip's position no
longer matters.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 5.4 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 13.7 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #2
wait for both, max 5 s    -> 2 ready after about 1000 ms
worker-1 makes the stock  -> ticket #3
worker-2 reads #3         -> #3's own bytes: true, 0.0 ms, 0.0 MiB allocated
both cooks busy, 300 ms   -> tickets #1 and #2
put the delivery          -> ticket #3, ready: true, 0.0 ms, 0.0 MiB allocated
cooks finished yet?       -> 0 of 2
8 tasks on 2 cooks        -> 8 done: 309.0 ms, 0.0 MiB allocated
saw the delivery's bytes  -> 8 of 8
picked by hand, 2 cooks   -> all 4 slips done after about 1000 ms
  the big slip's cook     -> 2 of 4 slips, done at about 1000 ms, then idle 0 ms
  the other cook          -> 2 of 4 slips, done at about 400 ms, then idle 600 ms
──── everything above: the first run's output, unchanged ────
one rail, a crew of 2     -> all 4 slips done after about 800 ms
  the big slip's cook     -> 1 of 4 slips, done at about 800 ms, then idle 0 ms
  the other cook          -> 3 of 4 slips, done at about 600 ms, then idle 200 ms
```

Read the new lines one by one:

- `one rail, a crew of 2 -> all 4 slips done after about 800 ms` — the
  same four slips, 200 ms sooner. 800 ms is the big slip alone; no kitchen could do
  better.

- `the big slip's cook -> 1 of 4 slips, done at about 800 ms` — one cook
  took the big slip and was busy with it the whole time.

- `the other cook -> 3 of 4 slips, done at about 600 ms, then idle 200 ms`
  — the other cook took slip 1, came back to the rail for slip 2, then
  slip 3. Nobody told it to; it was free first each time. Its last
  200 ms idle are not waste: there was nothing left to cook.

Which thread takes the big slip depends on which one reaches the rail
first, so the demo names cooks by their role ("the big slip's cook"), not
by thread name.

Four tests pin the crew. Append a test module to the end of
`src/pool.rs`:

```rust,ignore
// ──────── new code starts here (around line 87) ────────
#[cfg(test)]
mod tests {
    use super::Pool;
    use crate::ticket::{MemoryStore, Ticket, wait};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    /// The name of the thread running this code.
    fn cook_name() -> String {
        thread::current().name().unwrap_or("?").to_string()
    }

    #[test]
    fn a_crew_runs_every_slip_and_returns_its_result() {
        let crew = Pool::start(3, &MemoryStore::new());
        let squares: Vec<Ticket<u64>> = (1..=20_u64).map(|n| crew.submit(move || n * n)).collect();
        let total: u64 = squares.iter().map(|ticket| ticket.get().unwrap()).sum();
        assert_eq!(total, 2870);
    }

    #[test]
    fn a_free_cook_takes_the_next_slip_while_another_is_busy() {
        let crew = Pool::start(2, &MemoryStore::new());
        let (open_gate, gate) = mpsc::channel::<()>();
        let stuck = crew.submit(move || gate.recv().unwrap());
        let quick = crew.submit(|| 7_u64);
        // One cook is stuck behind the gate; only the other can cook `quick`.
        let (ready_now, _) = wait(vec![quick], 1, Duration::from_secs(10));
        open_gate.send(()).unwrap();
        assert_eq!(ready_now.len(), 1, "the free cook took the second slip");
        stuck.get().unwrap();
    }

    #[test]
    fn every_cook_in_the_crew_works_at_once() {
        let crew = Pool::start(3, &MemoryStore::new());
        let (tell, started) = mpsc::channel::<String>();
        let mut gates = Vec::new();
        let mut slips = Vec::new();
        for _ in 0..3 {
            let (open_gate, gate) = mpsc::channel::<()>();
            gates.push(open_gate);
            let tell = tell.clone();
            slips.push(crew.submit(move || {
                tell.send(cook_name()).unwrap();
                gate.recv().unwrap();
            }));
        }
        // All three slips must be cooking at the same time, each by a different cook.
        let mut names: Vec<String> = (0..3)
            .filter_map(|_| started.recv_timeout(Duration::from_secs(10)).ok())
            .collect();
        for open_gate in gates {
            let _ = open_gate.send(());
        }
        names.sort();
        assert_eq!(names, ["worker-1", "worker-2", "worker-3"]);
        for slip in slips {
            slip.get().unwrap();
        }
    }

    #[test]
    fn dropping_the_crew_finishes_every_queued_slip() {
        let done = Arc::new(AtomicUsize::new(0));
        let crew = Pool::start(2, &MemoryStore::new());
        for _ in 0..10 {
            let done = Arc::clone(&done);
            crew.execute(Box::new(move || {
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        drop(crew);
        assert_eq!(done.load(Ordering::SeqCst), 10);
    }
}
// ──────── new code ends here ────────
```

None of them sleeps to make a point; they use gates, as on Day 2:

- `a_crew_runs_every_slip_and_returns_its_result` — twenty slips on a
  crew of three, each squaring its number; the answers add up to
  1² + 2² + … + 20² = 2870, so every slip ran once and its result came
  back.

- `a_free_cook_takes_the_next_slip_while_another_is_busy` jams one cook
  behind a gate. The second slip can then only finish if the *other* cook
  takes it — so `wait` sees it ready. The generous 10 s limit only matters
  if the crew is broken.

- `every_cook_in_the_crew_works_at_once` sends three slips that each
  report the cook's name and then wait at a gate. All three names can
  arrive only if three different cooks are cooking at the same time.
  `filter_map` keeps only the values its closure returns as `Some`, and
  `.ok()` turns each `recv_timeout` result into `Some(name)` or `None` —
  so a broken crew fails the assertion instead of hanging.

- `dropping_the_crew_finishes_every_queued_slip` checks `Drop`: ten slips,
  then drop the crew, then all ten have run.

```bash
cargo test
```

```text
running 41 tests
test pool::tests::dropping_the_crew_finishes_every_queued_slip ... ok
test pool::tests::a_crew_runs_every_slip_and_returns_its_result ... ok
test pool::tests::a_free_cook_takes_the_next_slip_while_another_is_busy ... ok
test pool::tests::every_cook_in_the_crew_works_at_once ... ok
⋮ (the 37 earlier tests, all ok)

test result: ok. 41 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

All 37 earlier tests still pass, plus the four new ones.

**Section checkpoint:** `./scripts/compare.sh v1-day-04a`.

### Section B — anyone who takes slips

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/methods-and-traits/traits.html">traits</a>
<a href="https://doc.rust-lang.org/book/ch10-02-traits.html#default-implementations">default methods in a trait (Rust book)</a>
<a href="https://google.github.io/comprehensive-rust/generics/impl-trait.html"><code>impl Trait</code> arguments</a>
<a href="https://doc.rust-lang.org/std/primitive.slice.html#method.sort"><code>sort</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/vec/struct.Vec.html#method.dedup"><code>dedup</code> (std docs)</a>
<a href="https://doc.rust-lang.org/book/ch13-01-closures.html#closure-type-inference-and-annotation">a closure's return type (<code>|| -&gt; u64 { … }</code>, Rust book)</a>
</div>

Your crew takes plain slips. But Days 2 and 3 gave the cook two more
kinds: [held slips](story.md#held-slip) (`submit_with`) and slips that
read a pot in place (`submit_reading`). Do they work on the crew? Write
the scene and find out. Two edits to `src/main.rs`:

1. After the closing brace of `one_rail_for_the_crew` (around line 231),
   add:

   ```rust,ignore
   // ──────── new code starts here (around line 232) ────────
   /// Held slips and the pantry, through a crew of four.
   fn the_crew_and_the_pantry() {
       let pass = MemoryStore::new();
       let crew = Pool::start(4, &pass);
       let clock = Instant::now();
       let sauce = crew.submit(|| {
           thread::sleep(Duration::from_millis(300));
           "sauce".to_string()
       });
       let pasta = crew.submit_reading(&sauce, |sauce: &String| format!("pasta with {sauce}"));
       let quick: Vec<Ticket<String>> = (0..3).map(|_| crew.submit(cook_name)).collect();
       let (quick, _) = wait(quick, 3, Duration::from_secs(5));
       println!(
           "sauce cooking, pasta held -> {} quick slips done after about {} ms",
           quick.len(),
           round_to_100(clock.elapsed())
       );
       let dish = pasta.get().expect("the sauce never burns");
       println!(
           "{dish:<26}-> ready after about {} ms",
           round_to_100(clock.elapsed())
       );

       let stock = put(&pass, make_stock());
       let home = address(&stock.read().expect("a put value never burns"));
       let since = start_measuring();
       let readers: Vec<Ticket<(String, usize)>> = (0..8)
           .map(|_| {
               crew.submit_reading(&stock, |bytes: &Vec<u8>| {
                   thread::sleep(Duration::from_millis(50));
                   (cook_name(), address(bytes))
               })
           })
           .collect();
       let seen: Vec<(String, usize)> = readers
           .iter()
           .map(|reader| reader.get().expect("reading never burns"))
           .collect();
       println!("8 readers on a crew of 4  -> 8 done: {}", cost(since));
       let mut cooks: Vec<&String> = seen.iter().map(|(cook, _)| cook).collect();
       cooks.sort();
       cooks.dedup();
       let own = seen.iter().filter(|(_, at)| *at == home).count();
       println!(
           "cooks used, bytes seen    -> {} of 4 cooks, the stock's own bytes: {own} of 8",
           cooks.len()
       );
   }
   // ──────── new code ends here ────────
   ```

2. Add `the_crew_and_the_pantry();` as the last line of `main`.

The scene has two halves:

- **A held slip on the crew.** The sauce takes 300 ms. The pasta is held
  on the sauce. Three quick slips follow, each returning its cook's name
  (`crew.submit(cook_name)` hands over the function itself as the task).

- **The pantry through the crew.** The waiter puts the 64 MiB stock in the
  pantry, and eight slips read it in place. Each one dawdles 50 ms, so the
  slips spread across the crew, and reports its cook and the address it
  read. `sort` puts the eight cook names in order, and `dedup` then
  removes each name that repeats the one before it — so each cook's name
  is left once, and `cooks.len()` counts the cooks who took a share.

`cargo run` refuses to build (the first of two errors shown in full):

```text
error[E0599]: no method named `submit_reading` found for struct `Pool` in the current scope
   --> src/main.rs:243:22
    |
243 |     let pasta = crew.submit_reading(&sauce, |sauce: &String| format!("pasta with {sauce}"));
    |                      ^^^^^^^^^^^^^^ method not found in `Pool`
    |
   ::: src/pool.rs:14:1
    |
 14 | pub struct Pool {
    | --------------- method `submit_reading` not found for this struct
⋮ (a second error, the same, for the other `submit_reading` call)
error: could not compile `nano-ray-rs` (bin "nano-ray-rs") due to 2 previous errors
```

The crew only knows `execute` and `submit`. You could copy
`submit_reading` and `submit_with` into `Pool` too — about fifty more
lines, the same as `Worker`'s, kept in step by hand forever. And more
kinds of workers are coming: tomorrow's has a memory, Day 7's lives on
another machine.

Look at what those methods actually use from `self`: only two things —
where to send a job (the `Sender`) and which pass to use (the store).
Everything else is the same for every kind of worker. Rust has a tool for
exactly that: a **trait**. Call it `Submit` — in the story, *anyone who
takes slips*.

A trait lists methods. Some have no body: every type that implements the
trait must write those itself. Others have a body, written once in the
trait — *default methods* — and every implementing type gets them for
free. In outline (not code to type):

```text
pub trait Submit {
    fn queue(&self) -> &Sender<Job>;          no body: each kind of worker writes it
    fn store(&self) -> &Arc<MemoryStore>;     no body: each kind of worker writes it
    fn submit<T, F>(&self, task: F) -> …  { the body you already have, written once }
}
```

So `Submit` declares two methods without bodies (`queue`, `store`), and
four with bodies: `execute`, `submit`, `submit_reading`, `submit_with`.
For `Worker` nothing will change: `worker.submit(…)` runs the very same
code as before.

Six edits, in order. First `src/worker.rs`:

1. Inside `impl Worker`, delete the four methods `execute`, `submit`,
   `submit_reading`, and `submit_with` — lines 53–109, from the doc
   comment that starts `/// Queue` down to the closing brace of
   `submit_with`. `impl Worker` keeps only `start` and `start_in`; its own
   closing brace moves up to around line 52.

2. After that closing brace, add the trait — the four methods from step 1,
   with three small changes — and `Worker`'s side of it:

   ```rust,ignore
   // ──────── new code starts here (around line 53) ────────
   /// Analogy: anyone the waiter can hand an order slip to — a lone cook with a
   /// rail of their own, or a crew sharing one.
   ///
   /// Turning functions into tasks, written once for every kind of worker: an
   /// implementor only says where its jobs go and which store its results land in.
   pub trait Submit {
       /// The queue this worker (or crew) takes its jobs from.
       fn queue(&self) -> &Sender<Job>;

       /// The store its results land in.
       fn store(&self) -> &Arc<MemoryStore>;

       /// Queue `job` for the worker and return immediately. Fire and forget.
       fn execute(&self, job: Job) {
           // If the worker is gone the job is dropped unrun (its Promise reports it).
           let _ = self.queue().send(job);
       }

       /// Hand `task` to the worker and return a ticket for its result at once.
       fn submit<T, F>(&self, task: F) -> Ticket<T>
       where
           F: FnOnce() -> T + Send + 'static,
           T: Send + Sync + 'static,
       {
           let id = self.store().new_id();
           let promise = Promise::new(id, Arc::clone(self.store()));
           self.execute(Box::new(move || promise.keep(run_catching(task))));
           Ticket::new(id, Arc::clone(self.store()))
       }

       /// Like `submit`, but `task` reads the result behind `input` in place: it
       /// borrows the one stored value instead of getting a copy. Returns a
       /// ticket at once; the task joins the queue only once `input` has landed.
       /// If `input` failed, `task` never runs and its ticket carries the same
       /// error.
       fn submit_reading<A, T, F>(&self, input: &Ticket<A>, task: F) -> Ticket<T>
       where
           A: Send + Sync + 'static,
           F: FnOnce(&A) -> T + Send + 'static,
           T: Send + Sync + 'static,
       {
           let id = self.store().new_id();
           let promise = Promise::new(id, Arc::clone(self.store()));
           let input_copy = input.clone();
           let job: Job = Box::new(move || {
               // Already landed by the time this job runs, so `read` won't block.
               let outcome = match input_copy.read() {
                   Ok(value) => run_catching(move || task(&value)),
                   Err(error) => Err(error),
               };
               promise.keep(outcome);
           });
           let sender = self.queue().clone();
           input.when_ready(Box::new(move || {
               // As in `execute`: a job sent to a stopped worker is dropped unrun.
               let _ = sender.send(job);
           }));
           Ticket::new(id, Arc::clone(self.store()))
       }

       /// Like `submit_reading`, but `task` gets its own copy of the input value.
       fn submit_with<A, T, F>(&self, input: &Ticket<A>, task: F) -> Ticket<T>
       where
           A: Clone + Send + Sync + 'static,
           F: FnOnce(A) -> T + Send + 'static,
           T: Send + Sync + 'static,
       {
           self.submit_reading(input, move |value: &A| task(A::clone(value)))
       }
   }

   impl Submit for Worker {
       fn queue(&self) -> &Sender<Job> {
           self.sender.as_ref().expect("worker is running")
       }

       fn store(&self) -> &Arc<MemoryStore> {
           &self.store
       }
   }
   // ──────── new code ends here ────────
   ```

   The three changes inside the moved methods:

   - `pub` is gone from each method. A trait's methods are as public as
     the trait itself, which is `pub`.

   - `self.store` became `self.store()`. Inside the trait there is no
     field to read — only the method each kind of worker supplies.

   - `self.sender.…expect("worker is running")` became `self.queue()`.
     Each kind of worker decides for itself what "my queue" means:
     `Worker` and `Pool` both answer with their `Sender`, each with its
     own message for "already stopped".

3. Remove the four `pub(crate)` marks you added in section A, from
   `run_catching`, `Promise`, `Promise::new`, and `Promise::keep`. The
   trait lives in `worker.rs` itself, so nothing outside the file needs
   them any more.

Then `src/pool.rs`:

4. Replace its two `use crate::…` lines (lines 5–6) with:

   ```rust,ignore
   use crate::ticket::MemoryStore;
   use crate::worker::{Job, Submit};
   ```

5. Delete `Pool`'s `execute` and `submit` (around lines 42–58), so `impl
   Pool` keeps only `start`. After its closing brace (around line 41),
   add:

   ```rust,ignore
   // ──────── new code starts here (around line 42) ────────
   impl Submit for Pool {
       fn queue(&self) -> &Sender<Job> {
           self.sender.as_ref().expect("pool is running")
       }

       fn store(&self) -> &Arc<MemoryStore> {
           &self.store
       }
   }
   // ──────── new code ends here ────────
   ```

And `src/main.rs`:

6. Change `use worker::Worker;` (line 10) to `use worker::{Submit,
   Worker};`. A trait's methods can only be called where the trait itself
   is in scope.

`cargo run`:

```text
⋮ (Day 3's sixteen lines print first, unchanged)
picked by hand, 2 cooks   -> all 4 slips done after about 1000 ms
  the big slip's cook     -> 2 of 4 slips, done at about 1000 ms, then idle 0 ms
  the other cook          -> 2 of 4 slips, done at about 400 ms, then idle 600 ms
one rail, a crew of 2     -> all 4 slips done after about 800 ms
  the big slip's cook     -> 1 of 4 slips, done at about 800 ms, then idle 0 ms
  the other cook          -> 3 of 4 slips, done at about 600 ms, then idle 200 ms
──── everything above: section A's lines, unchanged ────
sauce cooking, pasta held -> 3 quick slips done after about 0 ms
pasta with sauce          -> ready after about 300 ms
8 readers on a crew of 4  -> 8 done: 120.1 ms, 0.0 MiB allocated
cooks used, bytes seen    -> 4 of 4 cooks, the stock's own bytes: 8 of 8
```

Read the new lines one by one:

- `sauce cooking, pasta held -> 3 quick slips done after about 0 ms` — the
  sauce keeps one cook busy for 300 ms. The pasta, held by
  `submit_reading` on the sauce's spot on the pass, keeps *no* cook busy,
  so the three quick slips went straight to the three free cooks.

- `pasta with sauce -> ready after about 300 ms` — the sauce landed, the
  pasta joined the one rail, and the first free cook made it. The
  `{dish:<26}` padding printed the dish itself as the label.

- `8 readers on a crew of 4 -> 8 done: … ms, 0.0 MiB allocated` — eight
  slips of 50 ms on four cooks: two rounds, about 100 ms. Not a byte
  copied: `submit_reading` lent each slip the pot in the pantry.

- `cooks used, bytes seen -> 4 of 4 cooks, the stock's own bytes: 8 of 8`
  — every cook in the crew took a share, and all eight read the very
  bytes the waiter put in the pantry.

Four tests pin it. First, the `use` lines of three test modules — the
trait's methods need the trait in scope there too:

1. In `src/worker.rs`'s `mod tests`, change `use super::{Promise, Worker};`
   (around line 224) to `use super::{Promise, Submit, Worker};`.

2. In `src/ticket.rs`'s `mod tests`, change `use crate::worker::Worker;`
   (around line 237) to `use crate::worker::{Submit, Worker};`.

3. In `src/pool.rs`'s `mod tests`, change `use crate::ticket::{MemoryStore,
   Ticket, wait};` (around line 82) to the two lines below. `put` and
   `Worker` are for the new tests:

   ```rust,ignore
       use crate::ticket::{MemoryStore, Ticket, put, wait};
       use crate::worker::{Submit, Worker};
   ```

4. Add four tests at the end of `src/pool.rs`'s `mod tests`, before the
   file's final closing `}`:

   ```rust,ignore
   mod tests {
       ⋮
       // ──────── new code starts here (around line 157) ────────
       #[test]
       fn a_held_slip_takes_no_cook_while_it_waits() {
           let store = MemoryStore::new();
           let crew = Pool::start(1, &store);
           // An input nobody has cooked yet: its spot on the pass stays empty.
           let sauce_id = store.new_id();
           let sauce: Ticket<u64> = Ticket::new(sauce_id, Arc::clone(&store));
           let pasta = crew.submit_with(&sauce, |sauce| sauce + 1);
           // The crew's only cook is free: the held slip is clipped to the pass,
           // not on the rail.
           assert_eq!(crew.submit(|| 7_u64).get().unwrap(), 7);
           store.put(sauce_id, Ok(Arc::new(41_u64)));
           assert_eq!(pasta.get().unwrap(), 42);
       }

       /// Where a byte vector's contents sit in memory.
       fn address(bytes: &[u8]) -> usize {
           bytes.as_ptr() as usize
       }

       #[test]
       fn the_crew_reads_a_put_value_in_place() {
           let store = MemoryStore::new();
           let crew = Pool::start(3, &store);
           let stock = put(&store, vec![7_u8; 1000]);
           let home = address(&stock.read().unwrap());
           let readers: Vec<Ticket<usize>> = (0..6)
               .map(|_| crew.submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
               .collect();
           for reader in readers {
               assert_eq!(reader.get().unwrap(), home);
           }
       }

       #[test]
       fn a_failed_input_on_the_crew_passes_the_error_on() {
           let crew = Pool::start(2, &MemoryStore::new());
           let burnt = crew.submit(|| -> u64 { panic!("the oven caught fire") });
           let next = crew.submit_with(&burnt, |x| x + 1);
           assert_eq!(
               next.get().unwrap_err().to_string(),
               "task failed: the oven caught fire"
           );
           // The cook who burnt it is still working.
           assert_eq!(crew.submit(|| 3_u64).get().unwrap(), 3);
       }

       /// Written once, for anything that takes order slips.
       fn double_on(slip_taker: &impl Submit, n: u64) -> u64 {
           slip_taker.submit(move || n * 2).get().unwrap()
       }

       #[test]
       fn one_function_drives_a_lone_cook_or_a_crew() {
           let store = MemoryStore::new();
           let cook = Worker::start_in("worker-0", &store);
           let crew = Pool::start(2, &store);
           assert_eq!(double_on(&cook, 20), 40);
           assert_eq!(double_on(&crew, 21), 42);
       }
       // ──────── new code ends here ────────
   }
   ```

What each one proves:

- `a_held_slip_takes_no_cook_while_it_waits` uses a crew of *one*. The
  sauce's spot on the pass stays empty — nobody cooks it — and yet a
  quick slip still gets cooked. If the held pasta had taken the only cook,
  that `get` would block forever. Then the test fills the sauce's spot by
  hand with `store.put`, and the pasta follows.

- `the_crew_reads_a_put_value_in_place` — six readers on three cooks, all
  at the pot's own address.

- `a_failed_input_on_the_crew_passes_the_error_on` — a burnt dish's note
  [travels down the chain](story.md#error-propagation) on the crew too, and
  the crew keeps cooking. `|| -> u64 { panic!(…) }` names the closure's
  return type, because a closure that only panics gives Rust nothing to
  infer it from.

- `one_function_drives_a_lone_cook_or_a_crew` — `double_on` takes
  `slip_taker: &impl Submit`: "a reference to *any* type that implements
  `Submit`". One function, written once, drives a `Worker` and a `Pool`.

```bash
cargo test
```

```text
running 45 tests
test pool::tests::a_held_slip_takes_no_cook_while_it_waits ... ok
test pool::tests::the_crew_reads_a_put_value_in_place ... ok
test pool::tests::a_failed_input_on_the_crew_passes_the_error_on ... ok
test pool::tests::one_function_drives_a_lone_cook_or_a_crew ... ok
⋮ (the 41 earlier tests, all ok)

test result: ok. 45 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
```

All 41 earlier tests pass — every Day 1–3 test now goes through the
trait — plus the four new ones.

**Section checkpoint:** `./scripts/compare.sh v1-day-04b`.

### Section C — more cooks, less time

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/references/slices.html">slices (<code>&amp;all[start..end]</code>)</a>
<a href="https://doc.rust-lang.org/std/thread/fn.available_parallelism.html"><code>available_parallelism</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/result/enum.Result.html#method.map_or"><code>map_or</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/primitive.usize.html#method.div_ceil"><code>div_ceil</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/primitive.u64.html#method.is_multiple_of"><code>is_multiple_of</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.flat_map"><code>flat_map</code> (std docs)</a>
</div>

Your crew finished the four slips 200 ms sooner. But look at *how* the cooks
in sections A and B were busy: `thread::sleep`. A sleeping thread uses no
processor at all, so a thousand sleeping cooks fit on one core. That
proves the scheduling works; it proves nothing about speed on real work.

So, a real job: **count the prime numbers from 1 to six million**,
testing each number the slow way. That is pure arithmetic — it keeps one
core busy and needs nothing else. The waiter puts the six million numbers
in the pantry once, cuts the job into 16 slips (each counts the primes in
one sixteenth of the numbers, read in place), and adds up the 16 answers.
Then the same job runs on a crew of 1, of 2, and of 4.

Three edits to `src/main.rs`:

1. Add `use std::sync::Arc;` above `use std::thread;` (line 7).

2. Add `more_cooks_less_time();` as the last line of `main`.

3. After the closing brace of `the_crew_and_the_pantry` (around line 283),
   add:

   ```rust,ignore
   // ──────── new code starts here (around line 284) ────────
   /// The slow job: count the primes among the numbers 1 to `PRIME_LIMIT`.
   const PRIME_LIMIT: u64 = 6_000_000;

   /// Whether `n` is prime, the slow way: try every divisor up to its square
   /// root. Pure arithmetic, so it keeps one CPU core busy and nothing else.
   fn is_prime(n: u64) -> bool {
       if n < 2 {
           return false;
       }
       let mut divisor = 2;
       while divisor * divisor <= n {
           if n.is_multiple_of(divisor) {
               return false;
           }
           divisor += 1;
       }
       true
   }

   /// How many of `numbers` are prime.
   fn count_primes(numbers: &[u64]) -> usize {
       numbers.iter().filter(|n| is_prime(**n)).count()
   }

   /// Share `n` of `all`, when `all` is cut into `slips` runs of equal length.
   fn share(all: &[u64], n: usize, slips: usize) -> &[u64] {
       let size = all.len().div_ceil(slips);
       let start = (n * size).min(all.len());
       let end = (start + size).min(all.len());
       &all[start..end]
   }

   /// Count the primes in `numbers` on `crew`, cut into `slips` slips that each
   /// read their share in place; then add up the answers.
   fn count_on(crew: &Pool, numbers: &Ticket<Vec<u64>>, slips: usize) -> usize {
       let tickets: Vec<Ticket<usize>> = (0..slips)
           .map(|n| {
               crew.submit_reading(numbers, move |all: &Vec<u64>| {
                   count_primes(share(all, n, slips))
               })
           })
           .collect();
       tickets
           .iter()
           .map(|ticket| ticket.get().expect("counting never burns"))
           .sum()
   }

   /// Start a crew of `size` cooks on `pass`, run the job on it, and return
   /// the answer and how long it took.
   fn time_on(
       pass: &Arc<MemoryStore>,
       numbers: &Ticket<Vec<u64>>,
       size: usize,
       slips: usize,
   ) -> (usize, f64) {
       let crew = Pool::start(size, pass);
       let clock = Instant::now();
       let primes = count_on(&crew, numbers, slips);
       (primes, ms_since(clock))
   }

   /// Milliseconds passed since `clock` started, with the fraction.
   fn ms_since(clock: Instant) -> f64 {
       clock.elapsed().as_secs_f64() * 1000.0
   }

   /// One slow job on crews of 1, 2 and 4 cooks, timed.
   fn more_cooks_less_time() {
       let cores = thread::available_parallelism().map_or(1, |n| n.get());
       println!("this machine              -> {cores} cores");
       let pass = MemoryStore::new();
       let numbers = put(&pass, (1..=PRIME_LIMIT).collect::<Vec<u64>>());
       println!("the job                   -> primes up to {PRIME_LIMIT}, as 16 slips");
       let mut alone_ms = 0.0;
       for size in [1, 2, 4] {
           let (primes, ms) = time_on(&pass, &numbers, size, 16);
           if size == 1 {
               alone_ms = ms;
           }
           println!(
               "a crew of {size}               -> {primes} primes in {ms:.0} ms, {:.2}x as fast as 1 cook",
               alone_ms / ms
           );
       }
   }
   // ──────── new code ends here ────────
   ```

A few details:

- `is_prime(n)` tries every divisor from 2 up to the square root of `n`.
  `n.is_multiple_of(divisor)` is `true` when `divisor` divides `n`
  exactly. Bigger numbers need more tries, so they cost more.

- In `count_primes`, `filter` hands its test a *reference* to each item,
  and `numbers.iter()` already yields references — so `n` is a reference
  to a reference, and `**n` follows both to the number itself.

- `share(all, n, slips)` is slip `n`'s part of the numbers. `div_ceil`
  divides and rounds *up* (1000 numbers in 16 slips: 63 each, the last one
  shorter), and `&all[start..end]` is a *slice*: a view of part of a
  vector, not a copy.

- `count_on` submits all the slips first and collects the answers after.
  Collecting each answer right after submitting its slip would leave
  three cooks idle — Ray's docs call that an anti-pattern: `ray.get` in a
  loop.

- `time_on` starts a crew, times the job on it, and returns the answer and
  the milliseconds. The crew is dropped at the end of `time_on`, so each
  size starts fresh. Starting the threads is not timed.

- `thread::available_parallelism()` asks the operating system how many
  tasks this machine can run at once — usually the number of cores. It
  returns a `Result`; `map_or(1, …)` turns an error into 1.

- `{:.2}x` prints the speedup with two digits after the decimal point.

`cargo run` (this one takes a few seconds):

```text
⋮ (Day 3's sixteen lines, then sections A and B's ten, print first, unchanged)
this machine              -> 18 cores
the job                   -> primes up to 6000000, as 16 slips
a crew of 1               -> 412849 primes in 2156 ms, 1.00x as fast as 1 cook
a crew of 2               -> 412849 primes in 1136 ms, 1.90x as fast as 1 cook
a crew of 4               -> 412849 primes in 619 ms, 3.48x as fast as 1 cook
```

Read it line by line:

- `this machine -> 18 cores` — the machine I ran this on: an Apple M5 Max
  laptop, whose 18 cores come in two kinds, 6 fast ones and 12 slower
  ones. Yours will differ.

- `the job -> primes up to 6000000, as 16 slips` — the numbers are in the
  pantry; `put` stored them before any timing started.

- `a crew of 1 -> 412849 primes in 2156 ms` — the baseline: one cook does
  all 16 slips, one after another.

- `a crew of 2 -> … 1136 ms, 1.90x` — a bit more than half the time.

- `a crew of 4 -> … 619 ms, 3.48x` — a bit more than a quarter, but
  clearly not a quarter.

Every answer is 412849: the slips add up to the same count, whoever
cooked them. The times are mine and will differ on your machine — and
from run to run on mine; the range across runs comes after the next
step.

Why not exactly 4×? Here is a guess you can test: the slips are not
equal. Slip 16 holds the numbers near six million, and each of those
needs more tries in `is_prime` than the small numbers in slip 1. The crew
is done only when its *last* slip is done. If the biggest slips come last,
three cooks may stand idle while the fourth finishes one.

Time the first and the last slip alone, then try smaller slips. At the end
of `more_cooks_less_time`, after the `for` loop's closing brace (around
line 369) — the `⋮` and the framing lines are already in your file:

```rust,ignore
fn more_cooks_less_time() {
    ⋮
    // ──────── new code starts here (around line 370) ────────
    let all = numbers.read().expect("a put value never burns");
    let clock = Instant::now();
    let first = count_primes(share(&all, 0, 16));
    let first_ms = ms_since(clock);
    let clock = Instant::now();
    let last = count_primes(share(&all, 15, 16));
    let last_ms = ms_since(clock);
    println!("slip 1 alone              -> {first} primes in {first_ms:.0} ms");
    println!("slip 16 alone             -> {last} primes in {last_ms:.0} ms");
    let (primes, ms) = time_on(&pass, &numbers, 4, 64);
    println!(
        "a crew of 4, 64 slips     -> {primes} primes in {ms:.0} ms, {:.2}x as fast as 1 cook",
        alone_ms / ms
    );
    // ──────── new code ends here ────────
}
```

`numbers.read()` borrows the six million numbers from the pantry, as on
Day 3. The two slips are timed right on the waiter's thread, no crew
involved. Then the whole job runs once more on a crew of 4, cut into 64
slips instead of 16.

`cargo run`:

```text
⋮ (Day 3's sixteen lines, then sections A and B's ten, print first, unchanged)
this machine              -> 18 cores
the job                   -> primes up to 6000000, as 16 slips
a crew of 1               -> 412849 primes in 2088 ms, 1.00x as fast as 1 cook
a crew of 2               -> 412849 primes in 1104 ms, 1.89x as fast as 1 cook
a crew of 4               -> 412849 primes in 599 ms, 3.49x as fast as 1 cook
──── everything above: the first run's five lines, with this run's timings ────
slip 1 alone              -> 31904 primes in 45 ms
slip 16 alone             -> 24057 primes in 178 ms
a crew of 4, 64 slips     -> 412849 primes in 571 ms, 3.65x as fast as 1 cook
```

Read the new lines one by one:

- `slip 1 alone -> 31904 primes in 45 ms` and `slip 16 alone -> 24057
  primes in 178 ms` — the guess holds. The last slip costs about four
  times the first, even though it holds fewer primes: its numbers are
  bigger, so every test takes more tries.

- `a crew of 4, 64 slips -> … 571 ms, 3.65x` — smaller slips, closer to
  4×. This is the *slip size*: how much of the job one slip carries —
  more slips means smaller slips. With 16 slips, the last round is four
  of the most expensive slips, and the cooks finish them at different
  times. With 64 slips, the last slips are a quarter of the size, so the
  cooks' finishing times bunch up and less time is spent idle at the end.

In this run the 16-slip crew of four reached 3.49×. I ran the finished
program ten times; together with the run before this step, that makes
eleven runs, the two shown here among them. Two cooks reached 1.81× to
1.93×, four cooks on 16 slips 3.27× to 3.63×, and four cooks on 64 slips
3.59× to 3.86× — and in all ten runs that had both, 64 slips beat 16.

How far from perfect is that? A perfect 4× would have taken 2088 ÷ 4 =
522 ms in this run. The 16-slip crew took 599 ms, 77 ms short; the
64-slip crew took 571 ms, 49 ms (about 9%) short. So unequal slips
explain part of the gap — smaller slips won back 28 of those 77 ms. The
remaining 49 ms I could not pin down. The waiter's own work is not it: I
timed the same 64 slips on a handful of numbers, and the whole round
trip — handing out the slips, the cooks taking them, adding up the
answers — took 0.1 to 0.4 ms. The most likely cause is the operating
system's choice of core: this chip has 6 fast cores and 12 slower ones,
nothing in our code says which a cook runs on, and other programs on the
laptop were running too.

Why not cut the job into a million slips, then? Because every slip has a
fixed cost: a ticket, a spot on the pass, a trip through the rail's lock,
and a cook waking up. With tiny slips that bookkeeping outweighs the
cooking — Ray's tip "avoid tiny tasks".

Three tests pin the job. Append a test module to the end of
`src/main.rs`, after the closing brace of `mib` (around line 414):

```rust,ignore
// ──────── new code starts here (around line 415) ────────
#[cfg(test)]
mod tests {
    use super::{count_on, count_primes, share};
    use crate::pool::Pool;
    use crate::ticket::{MemoryStore, put};

    #[test]
    fn count_primes_finds_the_primes_up_to_30() {
        let numbers: Vec<u64> = (1..=30).collect();
        // 2, 3, 5, 7, 11, 13, 17, 19, 23, 29
        assert_eq!(count_primes(&numbers), 10);
    }

    #[test]
    fn the_shares_cover_every_number_once() {
        let all: Vec<u64> = (1..=1000).collect();
        for slips in [1, 7, 16, 64] {
            let joined: Vec<u64> = (0..slips)
                .flat_map(|n| share(&all, n, slips).to_vec())
                .collect();
            assert_eq!(joined, all, "cut into {slips} slips");
        }
    }

    #[test]
    fn the_job_on_a_crew_gives_the_same_answer_as_one_cook() {
        let pass = MemoryStore::new();
        let crew = Pool::start(4, &pass);
        let all: Vec<u64> = (1..=10_000).collect();
        let expected = count_primes(&all);
        let numbers = put(&pass, all);
        assert_eq!(count_on(&crew, &numbers, 16), expected);
        assert_eq!(expected, 1229);
    }
}
// ──────── new code ends here ────────
```

- `count_primes_finds_the_primes_up_to_30` — the comment lists the ten
  answers.

- `the_shares_cover_every_number_once` — for four ways of cutting 1000
  numbers, the shares, laid end to end, are exactly the numbers: nothing
  lost, nothing counted twice. `flat_map` turns each share into a list
  of its own and joins those lists, end to end, into one.

- `the_job_on_a_crew_gives_the_same_answer_as_one_cook` — 16 slips on a
  crew of four give 1229, the known number of primes below 10,000. No
  timing: tests check answers, the demo measures speed.

```bash
cargo test
```

```text
running 48 tests
test meter::tests::the_meter_counts_a_fresh_allocation ... ok
test tests::count_primes_finds_the_primes_up_to_30 ... ok
test pool::tests::dropping_the_crew_finishes_every_queued_slip ... ok
test pool::tests::a_crew_runs_every_slip_and_returns_its_result ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test pool::tests::a_held_slip_takes_no_cook_while_it_waits ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test tests::the_shares_cover_every_number_once ... ok
test ticket::tests::put_moves_the_value_in_without_copying_it ... ok
test pool::tests::a_free_cook_takes_the_next_slip_while_another_is_busy ... ok
test ticket::tests::put_is_ready_at_once_and_needs_no_worker ... ok
test ticket::tests::read_shares_the_stored_value_and_get_copies_it ... ok
test pool::tests::a_failed_input_on_the_crew_passes_the_error_on ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test pool::tests::every_cook_in_the_crew_works_at_once ... ok
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test pool::tests::one_function_drives_a_lone_cook_or_a_crew ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test pool::tests::the_crew_reads_a_put_value_in_place ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::a_put_value_feeds_tasks_like_any_result ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test worker::tests::a_value_that_cannot_be_copied_can_still_be_read ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::put_values_and_task_results_share_one_numbering ... ok
test worker::tests::one_worker_reads_another_workers_result_in_place ... ok
test worker::tests::put_does_not_wait_for_a_busy_worker ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test tests::the_job_on_a_crew_gives_the_same_answer_as_one_cook ... ok
test worker::tests::reading_tasks_all_see_the_one_stored_value ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::submit_with_still_hands_each_task_its_own_copy ... ok
test worker::tests::workers_on_one_store_share_one_numbering ... ok
test worker::tests::wait_sees_results_from_every_worker_on_the_store ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok

test result: ok. 48 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

**Section checkpoint:** `./scripts/compare.sh v1-day-04c` — which is also
the finished day.

## 7. Run it

```bash
cargo run
```

## 8. Expected result

```text
⋮ (Day 3's sixteen lines print first, unchanged)
picked by hand, 2 cooks   -> all 4 slips done after about 1000 ms
  the big slip's cook     -> 2 of 4 slips, done at about 1000 ms, then idle 0 ms
  the other cook          -> 2 of 4 slips, done at about 400 ms, then idle 600 ms
one rail, a crew of 2     -> all 4 slips done after about 800 ms
  the big slip's cook     -> 1 of 4 slips, done at about 800 ms, then idle 0 ms
  the other cook          -> 3 of 4 slips, done at about 600 ms, then idle 200 ms
sauce cooking, pasta held -> 3 quick slips done after about 0 ms
pasta with sauce          -> ready after about 300 ms
8 readers on a crew of 4  -> 8 done: 120.1 ms, 0.0 MiB allocated
cooks used, bytes seen    -> 4 of 4 cooks, the stock's own bytes: 8 of 8
this machine              -> 18 cores
the job                   -> primes up to 6000000, as 16 slips
a crew of 1               -> 412849 primes in 2088 ms, 1.00x as fast as 1 cook
a crew of 2               -> 412849 primes in 1104 ms, 1.89x as fast as 1 cook
a crew of 4               -> 412849 primes in 599 ms, 3.49x as fast as 1 cook
slip 1 alone              -> 31904 primes in 45 ms
slip 16 alone             -> 24057 primes in 178 ms
a crew of 4, 64 slips     -> 412849 primes in 571 ms, 3.65x as fast as 1 cook
```

The whole evening in the kitchen, one stretch at a time:

- The first six of today's lines — the same four slips, picked by hand
  and then on a crew sharing one rail: 1000 ms with a cook idle for 600, then
  800 ms, the big slip alone. (`picked_by_hand`, `one_rail_for_the_crew`,
  `report`.)

- The next four — a crew of four keeps everything Days 2 and 3 built: a
  held slip takes no cook while it waits, and eight readers on four cooks
  share one pot in the pantry. (`the_crew_and_the_pantry`, through the
  `Submit` trait.)

- The last eight — the measurement: the same prime-counting job, 1.89×
  as fast on two cooks and 3.49× on four in this run — a bit more than a
  quarter of the time — and 3.65× once the slips are smaller. (`more_cooks_less_time`.)

What varies between runs: every millisecond figure in the last eight
lines, the `ms` of the `8 readers` line, and so every speedup. The other
lines round to the nearest 100 ms and stay the same; every count and
every number of primes is the same on every run. (One honest caveat, as
on Days 2 and 3: the first ten lines rely on `thread::sleep` to keep cooks
visibly busy. The tests rely on gates instead.)

## 9. Test it

Forty-eight tests now pin the engine — Day 3's 37, plus 11 new ones
today: the crew itself (section A, 4 tests), the `Submit` trait with held
slips and the pantry on the crew (section B, 4), and the prime-counting
job (section C, 3):

```bash
cargo test
```

It runs the same 48 tests as at the end of section C, in some order, and
ends:

```text
test result: ok. 48 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

(All 48 are unit tests; the program still takes no command-line input,
so there is nothing for a command-line test to drive.)

## 10. Break it deliberately

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/pattern-matching/let-control-flow/while-let.html"><code>while let</code></a>
</div>

Here is a shorter way to write `take_slips` — one you might well write
yourself. `while let Ok(job) = …` repeats its body for as long as the
pattern matches: here, as long as `recv` hands back `Ok(job)`. In
`src/pool.rs`, replace lines 58–66 — the whole `loop { … }` inside
`take_slips` — with:

```rust,ignore
    while let Ok(job) = rail.lock().unwrap().recv() {
        job();
    }
```

It reads the same: "while there is a next slip, cook it". It compiles
without a warning. `cargo run` (today's lines):

```text
⋮ (Day 3's sixteen lines print first, unchanged)
picked by hand, 2 cooks   -> all 4 slips done after about 1000 ms
  the big slip's cook     -> 2 of 4 slips, done at about 1000 ms, then idle 0 ms
  the other cook          -> 2 of 4 slips, done at about 400 ms, then idle 600 ms
one rail, a crew of 2     -> all 4 slips done after about 1400 ms
  the big slip's cook     -> 4 of 4 slips, done at about 1400 ms, then idle 0 ms
  the other cook          -> 0 of 4 slips, done at about 0 ms, then idle 1400 ms
sauce cooking, pasta held -> 3 quick slips done after about 300 ms
pasta with sauce          -> ready after about 300 ms
8 readers on a crew of 4  -> 8 done: 438.6 ms, 0.0 MiB allocated
cooks used, bytes seen    -> 1 of 4 cooks, the stock's own bytes: 8 of 8
this machine              -> 18 cores
the job                   -> primes up to 6000000, as 16 slips
a crew of 1               -> 412849 primes in 1984 ms, 1.00x as fast as 1 cook
a crew of 2               -> 412849 primes in 1978 ms, 1.00x as fast as 1 cook
a crew of 4               -> 412849 primes in 1937 ms, 1.02x as fast as 1 cook
slip 1 alone              -> 31904 primes in 41 ms
slip 16 alone             -> 24057 primes in 161 ms
a crew of 4, 64 slips     -> 412849 primes in 1993 ms, 1.00x as fast as 1 cook
```

Read it as a list of symptoms:

- `one rail, a crew of 2 -> all 4 slips done after about 1400 ms` —
  slower than picking by hand. `the big slip's cook -> 4 of 4 slips`, `the other cook
  -> 0 of 4 slips … idle 1400 ms`: one cook cooked everything.

- `sauce cooking, pasta held -> 3 quick slips done after about 300 ms` —
  the quick slips now wait for the sauce, with three cooks free.

- `8 readers on a crew of 4 -> 8 done: 438.6 ms` and `cooks used … 1 of 4
  cooks` — one cook read all eight, 50 ms after 50 ms.

- The crew sizes stop mattering: `1.00x` to `1.02x` as fast as one cook,
  for 2 cooks, 4 cooks, and 64 slips alike.

`cargo test` catches it — slowly, because the gated tests wait out their
10 s limits (lines trimmed to the interesting ones):

```text
running 48 tests
test pool::tests::a_free_cook_takes_the_next_slip_while_another_is_busy ... FAILED
test pool::tests::every_cook_in_the_crew_works_at_once ... FAILED

failures:

---- pool::tests::a_free_cook_takes_the_next_slip_while_another_is_busy stdout ----

thread 'pool::tests::a_free_cook_takes_the_next_slip_while_another_is_busy' (321313360) panicked at src/pool.rs:105:9:
assertion `left == right` failed: the free cook took the second slip
  left: 0
 right: 1

---- pool::tests::every_cook_in_the_crew_works_at_once stdout ----

thread 'pool::tests::every_cook_in_the_crew_works_at_once' (321313363) panicked at src/pool.rs:132:9:
assertion `left == right` failed
  left: ["worker-1"]
 right: ["worker-1", "worker-2", "worker-3"]

test result: FAILED. 46 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 20.01s
```

- `a_free_cook_takes_the_next_slip_while_another_is_busy` — `left: 0`:
  the second slip never got cooked while the first cook was stuck.

- `every_cook_in_the_crew_works_at_once` — only `worker-1` ever started.

What happened? `rail.lock()` returns a *guard*: the lock stays held for
as long as the guard exists. In `let next = rail.lock().unwrap().recv();`
the guard is a temporary that is dropped at the end of that `let`
statement, before the cook starts cooking. But a `while let` keeps the
temporaries of its condition alive until the end of the loop body. So
the cook holds the rail *while cooking*, and every other cook waits at
the lock. Four cooks, one at the stove at a time: a crew of one. The Rust
book's own thread pool
([chapter 21](https://doc.rust-lang.org/book/ch21-02-multithreaded.html))
warns about exactly this line, and the Rust reference spells out the rule
([temporary scopes](https://doc.rust-lang.org/reference/destructors.html#temporary-scopes)).

The lesson: **a shared queue is only as parallel as the time spent
holding its lock.** Hold it to take a slip, never to cook one. Ray's
scheduler faces the same rule at a bigger scale: deciding where a task
runs must be quick, because every task in the cluster passes through it.

Put the `loop` back and confirm `cargo test` is green again.

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
./scripts/compare.sh v1-day-04
```

**Inspecting instead of typing?** Jump to today's exact finished state,
poke at it, and come back:

```bash
git switch --detach v1-day-04
cargo test
git switch -          # back to where you were
```

**Either way**, today's complete change set — docs included — reads as one
diff:

```bash
git diff v1-day-03 v1-day-04
```

## 12. Recap

**What changed today:** you measured what choosing a cook by hand costs —
a cook idle for 600 ms while a slip waited on the other cook's rail. A
`Pool` puts a crew of threads behind one shared rail, an
`Arc<Mutex<Receiver>>` that each cook locks only long enough to take the
next slip, so the cook who is free first always gets it. The `Submit`
trait wrote `submit`, `submit_reading`, and `submit_with` once, for a lone
`Worker` and a `Pool` alike, so held slips and the pantry work on the crew
unchanged. And on real work — counting primes — a crew of four ran a slow
job in a bit more than a quarter of its time, closer to a quarter once
the slips were smaller. Eleven new tests (48 in all) pin it across three
section checkpoints (`v1-day-04a`, `v1-day-04b`, `v1-day-04c`), still with
no dependencies.

**Deliberately naive:** the crew's size is fixed when it starts; the
scheduler knows nothing about a slip — no priorities, no idea where its
inputs are, no idea whether it needs a GPU; every cook takes slips
through one lock, which is fine for slips of milliseconds but would
become the bottleneck for millions of tiny ones; you still choose the
slip size by hand; and a cook whose job crashes outside `submit` (a raw
`execute`) is gone for good, and the crew silently shrinks.

**Tomorrow's limitation, resolved on Day 5:** every cook in the crew is
interchangeable — any slip, any cook — so nothing one slip leaves behind
is there for the next. That is exactly what makes the crew fast, and
exactly what some work cannot live with: a counter, a cache, a model
loaded once and used a thousand times. On Day 5 a cook gets a memory and
a mailbox of their own — an **actor** — and a rail of one's own comes
back, this time on purpose. Tonight the kitchen closes with a crew on one
rail, one pass, and one pantry — see [the restaurant](story.md).
