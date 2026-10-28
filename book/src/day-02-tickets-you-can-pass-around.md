# Day 2: Tickets you can pass around

> **Day 2 of 17** · builds on `v1-day-01` · ≈ 2–2.5 h

| Section | Builds | Checkpoint |
|---------|--------|------------|
| A | Numbered tickets and the pass | `v1-day-02a` |
| B | A glance at the pass: `wait` with a time limit | `v1-day-02b` |
| C | An order that waits for another dish | `v1-day-02c` = `v1-day-02` |

**Pick your mode before starting** (explained fully in
[Setup](setup.md#4-choose-how-you-follow-along)):

```bash
# Build mode — start (or restart) your branch from yesterday's checkpoint.
# Already on my-course from Day 1? Keep going — no command needed.
git switch -c my-course v1-day-01

# Inspect mode — read along with the finished code instead:
git switch --detach v1-day-02a         # then 02b, 02c as you pass each section
```

<div class="callout">

**Today's typing budget** — so nothing sneaks up on you:

| Section | The learning | Wiring & tests (copy-friendly) |
|---------|--------------|--------------------------------|
| A | `ObjectId`, `MemoryStore`, the new `Ticket` · ~95 lines; `Promise`, `run_catching`, a new `submit` · ~50 lines | a new `main`, 5 tests |
| B | `MemoryStore::wait` + `wait` · ~45 lines | demo lines + 3 small helpers, 5 tests |
| C | `Slot`, `when_ready`, `submit_with` · ~75 lines | 3 tiny functions + demo lines, 7 tests |

Section A is the longest sitting of the day. Three
sections are three natural sittings, and every checkpoint is a save point
— nothing says a "day" must fit in one. Still **no dependencies**:
everything comes from Rust's standard library.

</div>

## 1. What you will have running

Yesterday a ticket could be read only once, by one holder. By the end of today, a ticket is **a number** — and a
number can be copied, shown twice, checked without waiting, and handed to
someone else.

`cargo run` will do three new things with it:

- **read the same result twice**, through two copies of one ticket — and
  read a crash report twice the same way;

- **wait for whichever of several results arrives first**, giving up after
  a time limit you choose;

- **hand a ticket to the next task as its input**, before the first task
  has even finished. The engine itself holds the second task back until
  its input exists, then runs it. If the input crashed, the second task
  never runs at all. It inherits the error instead.

That last move is how Ray chains work into pipelines. You describe the
whole chain up front, and the runtime runs each step the moment its input
is ready.

<div class="dl-concepts"><span>Ray concepts used here:</span>
<a href="https://docs.ray.io/en/latest/ray-core/objects.html">objects and ObjectRefs ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/api/doc/ray.get.html">ray.get ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/api/doc/ray.wait.html">ray.wait ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/tasks.html">passing refs into tasks ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html">how task errors travel ↗</a>
</div>

## 2. What you will be seeing in the output

A sneak peek at the end of the day — the exact output you will produce,
with a plain-English note under each group of lines. If a note doesn't
fully land yet, that's fine. The rest of the day builds every piece:

```text
submit slow_square(7) -> ticket #1
clone the ticket      -> ticket #1
get ticket #1         -> 49
get the copy          -> 49
submit a burning task -> ticket #2

thread 'worker-0' (319781764) panicked at src/main.rs:35:5:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get ticket #2 (try 1) -> error: task failed: the oven caught fire
get ticket #2 (try 2) -> error: task failed: the oven caught fire
submit 3 orders       -> tickets [#3, #4, #5]
wait for 1, max 5 s   -> ready [#3], pending [#4, #5] at about 300 ms
wait for 1, max 0.1 s -> ready [], pending [#4, #5] at about 400 ms
wait for 2, max 5 s   -> ready [#4, #5], pending [] at about 900 ms
wait on the burnt one -> ready [#2] (a failure counts as done)
submit slow_square(3) -> ticket #6
submit add_one(#6)    -> ticket #7
submit double(#7)     -> ticket #8
submit times_ten(#6)  -> ticket #9
is #6 ready yet?      -> false
    cook: add_one(9) = 10
    cook: times_ten(9) = 90
    cook: double(10) = 20
get ticket #8         -> 20
get ticket #9         -> 90
submit slow_burn      -> ticket #10
submit add_one(#10)   -> ticket #11

thread 'worker-0' (319781764) panicked at src/main.rs:35:5:
the oven caught fire
get ticket #11        -> error: task failed: the oven caught fire
```

- The first four lines — a slow job is handed off and comes back as
  number `#1`. A copy of the ticket carries the same number, and you can
  read both the original and the copy. The answer, 49, is still there the
  second time.

- The crash block and the two `try` lines — a job that crashes on purpose.
  Its failure is kept under its number too, so it reads the same both
  times.

- `submit 3 orders` and the three `wait for` lines — three slow jobs at
  once. First the program asks "tell me when at least one is done," and
  job `#3` arrives after about 300 ms. Then it asks for "one more, but give
  up after 0.1 s." Nothing is done in time, so it gives up empty-handed.
  Last it asks for "both of the rest," and they arrive at about 900 ms.

- `wait on the burnt one` — a crashed job counts as *finished*. Its
  answer is an error, but it is an answer.

- `submit slow_square(3)` down to `is #6 ready yet? -> false` — four jobs
  handed off in a row. Three of them take *another job's number* as their
  input. They do so before that input exists, as the last line proves.

- The three indented `cook:` lines — the jobs that were waiting on an
  input run as soon as it arrives. Notice the order. `times_ten` runs
  before `double`, although `double` was handed off first. That is because
  `double` needed `add_one`'s answer.

- `get ticket #8 -> 20` and `get ticket #9 -> 90` — (3 × 3 + 1) × 2 = 20
  and 3 × 3 × 10 = 90.

- The last five lines — a job that crashes, and a second job that needs
  its answer. The second job never runs (no `cook: add_one` line appears).
  Its number receives the first job's error instead.

## 3. Why we need it

**The story so far — remember [the restaurant](story.md):** one
[cook](story.md#worker) works through [order slips](story.md#task) from a rail. Pinning a
slip gives the waiter a [ticket](story.md#ticket) on the spot, and showing
it at the pass — the counter where finished dishes come out — is
[get](story.md#get): wait there until the dish is ready. A burnt dish
comes back as a [note](story.md#task-error) instead of closing the
kitchen.

Today's scene: **the pass gets numbered spots.** Right now each dish
travels to the waiter in a private envelope. Only the one ticket holder
can open it, and only once. That is fine for one waiter collecting one
dish. It fails the moment work starts to *connect*:

- **A result needed in two places.** Two later orders both need the same
  sauce. With yesterday's ticket, whoever opens the envelope first takes
  the sauce. The second order has nothing left to open.

- **Several dishes in the air.** The waiter has three tickets and wants
  whichever dish is ready first. Or they want to give up after a while and
  check on the dining room. Yesterday's `get` can only stand at the counter
  for one particular dish, for as long as it takes.

- **An order that needs another dish.** The waiter wants to order "the
  sauce" and "pasta with that sauce" in one trip. The pasta slip can't be
  cooked until the sauce exists, and the waiter should not have to stand
  around and come back later to place it.

So today's three sections move the results *out* of the envelopes and
onto the pass, under their ticket numbers. Section A builds the numbered
pass and the copyable ticket. Section B teaches the waiter to glance at
the pass with a time limit. Section C lets an order slip wait, clipped to
the spot of the dish it needs.

## 4. Mental model

Walk the counter, one station per section:

- **Numbered spots on the pass (section A).** When the cook finishes a
  dish, it goes on the pass under its ticket number and *stays there*.
  The ticket is no longer an envelope. It is a number, and anyone holding
  a copy of the number can come and look. Looking doesn't remove the dish.
  The pass also has a **bell**. Each time a dish lands, the cook rings it,
  and everyone waiting at the counter checks whether their number came up.

- **The cook's stub (section A).** When a slip goes on the rail, the
  cook keeps a stub with the same number. The dish may cook or burn, or
  the slip may be thrown away unrun because the cook has gone home.
  Whatever happens, the stub guarantees that *something* lands on the pass
  under that number, so no waiter waits forever.

- **A glance at the pass (section B).** The waiter holding three tickets
  stands at the pass for at most, say, five seconds, and leaves with
  whichever dishes are up — sorted into "ready" and "still pending."

- **A slip that needs another dish (section C).** The pasta slip gets
  clipped to the *empty* spot where the sauce will land. When the sauce
  lands, the cook unclips the slip and pins it to the rail. If the sauce
  burned instead, the pasta is never cooked. The burnt-dish note is
  copied onto the pasta's spot.

```text
main (the waiter)                     the pass             worker-0 (the cook)
    │ submit(slow_square(3))                                   │
    │──── slip #6 on the rail ─────────────────────────────────▶ cooking #6 …
    │ order add_one, made from #6                              │
    │──── slip #7 clipped to spot #6 ──▶ #6: [slip #7]          │
    │◀─── ticket #7, at once                                   │
    │ get(#7)  — waits at the pass                             │
    │                                  #6: 9  ◀── dish lands ──┤ done
    │                                  (bell rings; slip #7    │
    │                                   moves to the rail) ────▶ cooking #7
    │                                  #7: 10 ◀── dish lands ──┤ done
    │◀─── 10 ───────────────────────── (bell rings)            │
```

Read the diagram top to bottom, because time flows downward. The waiter
is on the left, the cook on the right, and the pass sits between them.

## 5. Ray concepts introduced

- **ObjectRef = an id plus where to look.** In Ray, `f.remote()` returns
  an `ObjectRef`, which is an object id. The result itself lives in a
  store, keyed by that id. Today's `Ticket<T>` is exactly that: an
  `ObjectId` plus a handle to the store. Copying an `ObjectRef` copies the
  id, never the result.

- **The in-process memory store.** Ray keeps small task results in a
  table inside the process that submitted the task. Larger ones go to the
  shared-memory object store of Day 3. Today's `MemoryStore` is that
  table, named after it.

- **`ray.get` reads, it doesn't take.** You can `ray.get` the same ref
  many times, and the object stays in the store. Today's `get(&self)`
  borrows the ticket instead of consuming it.

- **`ray.wait(refs, num_returns, timeout)`** returns `(ready,
  not_ready)`: at least `num_returns` ready refs, unless the timeout runs
  out first. Today's `wait` has the same shape and the same
  order-preserving split, and it also counts a failed task as ready. One
  difference: Ray's timeout may be `None` ("wait forever"), but yours is
  always a number.

- **Passing refs into tasks.** `g.remote(f.remote())` passes the *ref*.
  Ray holds `g` back until the value exists, then calls `g` with the
  value, so `g` never sees a ref. Today's `submit_with` does the same for
  one input.

- **Errors propagate.** If `f` failed, `g` does not run, and
  `ray.get` on `g`'s ref raises `f`'s error. Today's dependent ticket
  carries the original `TaskError`, message intact.

## 6. Build it, section by section

### Section A — numbered tickets and the pass

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/modules/filesystem.html">modules in files (<code>mod ticket;</code>)</a>
<a href="https://google.github.io/comprehensive-rust/modules/paths.html">paths (<code>crate::…</code>)</a>
<a href="https://google.github.io/comprehensive-rust/std-types/hashmap.html"><code>HashMap</code></a>
<a href="https://google.github.io/comprehensive-rust/concurrency/shared-state/arc.html"><code>Arc</code></a>
<a href="https://google.github.io/comprehensive-rust/concurrency/shared-state/mutex.html"><code>Mutex</code></a>
<a href="https://doc.rust-lang.org/std/sync/struct.Condvar.html"><code>Condvar</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/sync/atomic/index.html">atomics (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/memory-management/copy-types.html"><code>Copy</code> types</a>
<a href="https://google.github.io/comprehensive-rust/user-defined-types/aliases.html">type aliases</a>
<a href="https://doc.rust-lang.org/std/marker/struct.PhantomData.html"><code>PhantomData</code> (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/borrowing/shared.html">borrowing (<code>&amp;self</code>)</a>
<a href="https://google.github.io/comprehensive-rust/error-handling/try.html">the <code>?</code> operator</a>
<a href="https://google.github.io/comprehensive-rust/iterators/collect.html"><code>map</code> &amp; <code>collect</code></a>
</div>

Your Day 1 ticket works — so why touch it? Try reading one twice. Add a
second `get` right under the first one in Day 1's `main`:

```rust,ignore
    let answer = ticket.get().expect("slow_square never fails");
    let again = ticket.get().expect("slow_square never fails");
```

`cargo build` refuses. (A warning about the unused `again` follows, but
it doesn't matter here.)

```text
error[E0382]: use of moved value: `ticket`
  --> src/main.rs:22:17
   |
17 |     let ticket = worker.submit(|| slow_square(7));
   |         ------ move occurs because `ticket` has type `Ticket<u64>`, which does not implement the `Copy` trait
...
21 |     let answer = ticket.get().expect("slow_square never fails");
   |                         ----- `ticket` moved due to this method call
22 |     let again = ticket.get().expect("slow_square never fails");
   |                 ^^^^^^ value used here after move
   |
note: `Ticket::<T>::get` takes ownership of the receiver `self`, which moves `ticket`
  --> src/worker.rs:86:16
   |
86 |     pub fn get(self) -> Result<T, TaskError> {
   |                ^^^^
```

Yesterday that was a feature: you cannot collect the same dish twice.
But it is also a wall. The dish lives *inside* the ticket, in its private
channel. Once you take the dish out, it is gone, and no copy of the ticket
could ever exist to read it again. (Delete the extra line before
moving on.)

The fix is to separate the two things the ticket mixes up:

- **the result** goes onto a shared table, the pass, filed under a
  number;

- **the ticket** becomes that number, plus a pointer to the pass.

A number can be copied freely, and reading the table doesn't empty it.

Two new tools make a table safe to share between threads, and one more
lets a waiting thread sleep:

- `Arc` ("atomically reference-counted") lets several owners hold the
  *same* value. Cloning an `Arc` copies a pointer, never the value. The
  value is freed when the last owner lets go. The worker, every task, and
  every ticket will hold an `Arc` to the same pass.

- `Mutex` (a lock) makes threads take turns. `lock()` waits until no one
  else holds the lock, then hands you a *guard*. The guard is a value that
  stands for "I hold the lock right now," and it lets you reach the data
  inside. When the block or statement holding the guard ends, the guard is
  dropped, and dropping it opens the lock again automatically.

- `Condvar` (a condition variable) is the pass's bell. A thread holding
  the lock can call `wait(guard)`. That call releases the lock, sleeps
  until someone calls `notify_all()`, then takes the lock back. The bell
  says only "something changed," never what. So every sleeper re-checks
  its own number and goes back to sleep if it isn't there yet. That is why
  the code below waits inside a `loop`.

Results come in every type, a `u64` here and a `String` there. But one
table must hold them all. So the pass stores each value as `Arc<dyn Any +
Send + Sync>`: "a shared box holding *something*, type unknown." You met
`dyn Any` on Day 1 when unwrapping a panic's message. `Sync` is `Send`'s
twin and means "safe for several threads to *read* at once."

The ticket is what remembers the real type `T`. So when `get` takes a
box off the pass, it opens the box with `downcast_ref::<T>()`, which
means "give me the `T` inside." It is the same tool `from_panic` used on
Day 1.

Make nine edits, in order. The first creates a new file. The rest rework
`src/main.rs` and `src/worker.rs`.

1. Create `src/ticket.rs` with the ticket number, the pass, and the new
   ticket:

   ```rust,ignore
   use std::any::Any;
   use std::collections::HashMap;
   use std::fmt;
   use std::marker::PhantomData;
   use std::sync::atomic::{AtomicU64, Ordering};
   use std::sync::{Arc, Condvar, Mutex};

   use crate::worker::TaskError;

   /// Analogy: the number printed on an order ticket.
   ///
   /// A unique id naming one task's result, whether or not it exists yet.
   #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
   pub struct ObjectId(u64);

   impl fmt::Display for ObjectId {
       fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
           write!(f, "#{}", self.0)
       }
   }

   /// What a finished task left behind: its value (type hidden), or why it failed.
   pub type Outcome = Result<Arc<dyn Any + Send + Sync>, TaskError>;

   /// Analogy: the pass — the counter where finished dishes wait under their
   /// ticket numbers until someone collects them.
   ///
   /// A shared table from object id to outcome. A condition variable wakes
   /// every waiting thread whenever a new outcome lands.
   pub struct MemoryStore {
       results: Mutex<HashMap<ObjectId, Outcome>>,
       landed: Condvar,
       next_id: AtomicU64,
   }

   impl MemoryStore {
       /// An empty pass, ready to be shared between threads.
       pub fn new() -> Arc<MemoryStore> {
           Arc::new(MemoryStore {
               results: Mutex::new(HashMap::new()),
               landed: Condvar::new(),
               next_id: AtomicU64::new(1),
           })
       }

       /// Hand out the next unused ticket number.
       pub fn new_id(&self) -> ObjectId {
           ObjectId(self.next_id.fetch_add(1, Ordering::Relaxed))
       }

       /// Record the outcome for `id` and wake everyone waiting.
       pub fn put(&self, id: ObjectId, outcome: Outcome) {
           self.results.lock().unwrap().insert(id, outcome);
           self.landed.notify_all();
       }

       /// Block until `id` has an outcome, then return a copy of it.
       pub fn get(&self, id: ObjectId) -> Outcome {
           let mut results = self.results.lock().unwrap();
           loop {
               if let Some(outcome) = results.get(&id) {
                   return outcome.clone();
               }
               // Releases the lock while asleep; re-takes it before returning.
               results = self.landed.wait(results).unwrap();
           }
       }
   }

   /// Analogy: the numbered order ticket — and now that it is only a number,
   /// you can photocopy it and hand copies around.
   ///
   /// An object id plus the store it lives in. Cloning copies the claim, not
   /// the result; every copy reads the same outcome.
   #[derive(Clone)]
   pub struct Ticket<T> {
       id: ObjectId,
       store: Arc<MemoryStore>,
       result_type: PhantomData<T>,
   }

   impl<T> Ticket<T> {
       /// A ticket for `id`, whose result will land in `store`.
       pub fn new(id: ObjectId, store: Arc<MemoryStore>) -> Ticket<T> {
           Ticket {
               id,
               store,
               result_type: PhantomData,
           }
       }

       /// The ticket's number.
       pub fn id(&self) -> ObjectId {
           self.id
       }
   }

   impl<T: Clone + 'static> Ticket<T> {
       /// Wait until the task finishes, then hand over a copy of its result.
       /// The result stays in the store, so any copy of the ticket can `get` it.
       pub fn get(&self) -> Result<T, TaskError> {
           let value = self.store.get(self.id)?;
           let value = value
               .downcast_ref::<T>()
               .expect("a ticket's type always matches its task's result");
           Ok(value.clone())
       }
   }
   ```

   A few details are worth a second look:

   - `ObjectId(u64)` is a *tuple struct*: a named wrapper around one
     number, so an id can never be confused with any other `u64`. Its
     `derive` line asks for the abilities a key needs. `Copy` makes
     copying it as cheap as copying the number, with no `.clone()` needed.
     `Eq` and `Hash` let a `HashMap` use it as a key. `Display` prints it as
     `#1`.

   - `next_id` is an `AtomicU64`, a number that several threads may
     update at once without a lock. `fetch_add(1, …)` returns the current
     value and bumps it in one indivisible step, so no two tasks ever get
     the same number. (`Ordering::Relaxed` says the number only needs to
     be unique, not to coordinate anything else.)

   - `Outcome` is a *type alias*: a short name for the long
     `Result<Arc<dyn Any + Send + Sync>, TaskError>`, which appears
     several times today.

   - `Ticket` holds no `T` at all, only an id. Yet it must remember which
     type its result has. `PhantomData<T>` is a zero-size marker field for
     exactly that: "this struct is *about* a `T`."

   - `get` now takes `&self`, a borrow, so reading never consumes the
     ticket. It lives in its own `impl` block with the bound `T: Clone`,
     because it hands out a *copy* of the result. The original stays on
     the pass.

   - The `?` after `self.store.get(self.id)` means "if this is an error,
     return it from `get` right now."

2. In `src/main.rs`, declare the new module: add `mod ticket;` as the
   first line, above `mod worker;`. That line tells Rust "there is a
   module named `ticket`, and its code is in `src/ticket.rs`." It works
   exactly the way `mod worker;` found `src/worker.rs` on Day 1.

3. In `src/worker.rs`, replace the import lines at the top of the file
   (the first five lines) with these. `Receiver` is gone, because tickets
   no longer hold a channel. The last line reaches into the new module.
   `crate::` means "starting from the top of this program," so
   `crate::ticket::Ticket` is "the `Ticket` inside the `ticket` module."
   (`src/ticket.rs` reaches back the same way, with
   `crate::worker::TaskError`.)

   ```rust,ignore
   use std::any::Any;
   use std::fmt;
   use std::panic::{self, AssertUnwindSafe};
   use std::sync::Arc;
   use std::sync::mpsc::{self, Sender};
   use std::thread::{self, JoinHandle};

   use crate::ticket::{MemoryStore, ObjectId, Outcome, Ticket};
   ```

4. Still in `src/worker.rs`, give the cook a pass. Add a third field to
   `pub struct Worker`, after `handle` (around line 21):

   ```rust,ignore
       store: Arc<MemoryStore>,
   ```

5. Fill it in at the end of `Worker::start`, after the
   `handle: Some(handle),` line (around line 40):

   ```rust,ignore
               store: MemoryStore::new(),
   ```

6. Replace the whole `submit` method (around line 50) with this version.
   The old body made a private channel. The new one asks the pass for a
   number instead:

   ```rust,ignore
       /// Hand `task` to the worker and return a ticket for its result at once.
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
   ```

7. After the closing brace of `impl Drop for Worker` (around line 73),
   add a small function. Yesterday's `catch_unwind` call moves into it,
   because section C needs it a second time:

   ```rust,ignore
   // ──────── new code starts here (around line 73) ────────
   /// Run `task`, turning a panic into a `TaskError` instead of a dead thread.
   fn run_catching<T, F>(task: F) -> Outcome
   where
       F: FnOnce() -> T,
       T: Send + Sync + 'static,
   {
       match panic::catch_unwind(AssertUnwindSafe(task)) {
           Ok(value) => Ok(Arc::new(value)),
           Err(payload) => Err(TaskError::from_panic(payload)),
       }
   }
   // ──────── new code ends here ────────
   ```

8. **Delete** Day 1's `pub struct Ticket<T>` and its `impl<T>
   Ticket<T>` block (they now live in `src/ticket.rs`), and put the
   cook's stub — `Promise` in the code — in their place:

   ```rust,ignore
   // ──────── new code starts here (around line 85) ────────
   /// Analogy: the cook's stub of the order slip — whatever happens to the
   /// order, something lands on the pass under that number.
   ///
   /// The duty to store one task's outcome. If the job is thrown away unrun,
   /// `Drop` stores a "worker stopped" error, so no `get` waits forever.
   struct Promise {
       id: ObjectId,
       store: Arc<MemoryStore>,
       kept: bool,
   }

   impl Promise {
       fn new(id: ObjectId, store: Arc<MemoryStore>) -> Promise {
           Promise {
               id,
               store,
               kept: false,
           }
       }

       /// Store the task's outcome under the promised id.
       fn keep(mut self, outcome: Outcome) {
           self.store.put(self.id, outcome);
           self.kept = true;
       }
   }

   impl Drop for Promise {
       fn drop(&mut self) {
           if !self.kept {
               let error = TaskError {
                   message: "worker stopped before the task finished".to_string(),
               };
               self.store.put(self.id, Err(error));
           }
       }
   }
   // ──────── new code ends here ────────
   ```

9. Last, a task's error must now be *copied* out of the pass rather than
   moved out of an envelope. Add `Clone` to `TaskError`'s derive line
   (around line 124):

   ```rust,ignore
   #[derive(Clone, Debug)]
   ```

Why does the stub exist? Remember Day 1's *"worker stopped before the
task finished"*. It came for free, because a thrown-away job dropped its
channel's sender, and `recv` noticed. Now there is no channel. If a job
were thrown away unrun, nothing would ever land under its number, and its
`get` would wait for the bell forever. The stub closes that hole with
the `Drop` hook you met on Day 1. `keep` stores the outcome and marks the
stub kept. If a stub is dropped *without* being kept, its `drop` stores
the "worker stopped" error instead. Either way, something lands.

Now a demo of the new tickets. Replace `src/main.rs` with:

```rust,ignore
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
```

`slow_burn` is a plain function, so `worker.submit(slow_burn)` hands it
over by name. A task that takes no arguments needs no `|| …` closure. It
sleeps first, so its crash report prints *after* the `submit` line, not
in the middle of it.

`cargo run`:

```text
submit slow_square(7) -> ticket #1
clone the ticket      -> ticket #1
get ticket #1         -> 49
get the copy          -> 49
submit a burning task -> ticket #2

thread 'worker-0' (319767850) panicked at src/main.rs:17:5:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get ticket #2 (try 1) -> error: task failed: the oven caught fire
get ticket #2 (try 2) -> error: task failed: the oven caught fire
```

Read it line by line:

- `submit slow_square(7) -> ticket #1` — the waiter pins a slip and gets
  ticket number 1, at once. `submit` asked the pass for a fresh number
  with `new_id`, and the ticket's `Display` printed it as `#1`.

- `clone the ticket -> ticket #1` — a photocopy of the ticket, with the
  same number. `clone` copied the id and the `Arc` pointer to the pass. No
  dish was copied, because none exists yet.

- `get ticket #1 -> 49` — the waiter waits at the pass for spot #1.
  `Ticket::get` called `MemoryStore::get`, which slept on the bell until
  the cook's stub put 49 there with `keep`.

- `get the copy -> 49` — the copy reads the same dish, which is still on
  the pass. Yesterday this line could not compile.

- `submit a burning task -> ticket #2`, then the crash report — the
  dish burns on the cook's thread, and Rust prints its standard crash
  report as on Day 1. `run_catching` caught it, and the stub stored a
  `TaskError` under #2.

- `get ticket #2 (try 1)` and `(try 2)` — the burnt-dish note is read
  twice, identically. `get` cloned the stored error each time.

Five new tests pin today's behavior. The pass gets its own tests, and
they need no worker at all, because they put results in by hand. Append to
`src/ticket.rs`:

```rust,ignore
#[cfg(test)]
mod tests {
    use super::{MemoryStore, Outcome, Ticket};
    use std::sync::Arc;
    use std::thread;

    fn ready(value: u64) -> Outcome {
        Ok(Arc::new(value))
    }

    #[test]
    fn ids_are_unique_and_count_up_from_one() {
        let store = MemoryStore::new();
        let ids: Vec<String> = (0..3).map(|_| store.new_id().to_string()).collect();
        assert_eq!(ids, ["#1", "#2", "#3"]);
    }

    #[test]
    fn get_blocks_until_another_thread_puts_the_result() {
        let store = MemoryStore::new();
        let id = store.new_id();
        let ticket: Ticket<u64> = Ticket::new(id, Arc::clone(&store));
        let cook = thread::spawn(move || store.put(id, ready(42)));
        assert_eq!(ticket.get().unwrap(), 42);
        cook.join().unwrap();
    }

    #[test]
    fn every_copy_of_a_ticket_reads_the_same_result() {
        let store = MemoryStore::new();
        let id = store.new_id();
        let ticket: Ticket<u64> = Ticket::new(id, Arc::clone(&store));
        let copy = ticket.clone();
        store.put(id, ready(9));
        assert_eq!(ticket.get().unwrap(), 9);
        assert_eq!(ticket.get().unwrap(), 9);
        assert_eq!(copy.get().unwrap(), 9);
    }
}
```

One new tool appears in `ids_are_unique_and_count_up_from_one`:
`(0..3).map(…)` produces three values one at a time, and `.collect()`
gathers them into a collection. Here that is the `Vec<String>` named on
the left.

Then two tests in `src/worker.rs`:

1. Replace the first line inside `mod tests` (around line 152), `use
   super::Worker;`, with these two lines:

   ```rust,ignore
       use super::{Promise, Worker};
       use crate::ticket::{MemoryStore, Ticket};
   ```

2. Replace the final closing `}` of the file (the end of `mod tests`)
   with these two tests and a new closing `}`:

   ```rust,ignore
       #[test]
       fn a_burnt_dish_reads_the_same_through_every_copy() {
           let worker = Worker::start("worker-0");
           let burnt = worker.submit(|| -> u64 { panic!("the oven caught fire") });
           let copy = burnt.clone();
           let expected = "task failed: the oven caught fire";
           assert_eq!(burnt.get().unwrap_err().to_string(), expected);
           assert_eq!(copy.get().unwrap_err().to_string(), expected);
       }

       #[test]
       fn a_job_thrown_away_unrun_reports_worker_stopped() {
           let store = MemoryStore::new();
           let id = store.new_id();
           let ticket: Ticket<u64> = Ticket::new(id, Arc::clone(&store));
           drop(Promise::new(id, store));
           assert_eq!(
               ticket.get().unwrap_err().to_string(),
               "task failed: worker stopped before the task finished"
           );
       }
   }
   ```

The second test drops a stub without keeping it, which is exactly what
happens to a job thrown away unrun. Then it checks that the ticket
reports "worker stopped" instead of hanging.

```bash
cargo test
```

```text
running 12 tests
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
```

All seven of Day 1's tests still pass untouched. `submit` and `get` kept
their names and their behavior, even though everything behind them
moved.

**Section checkpoint:** `./scripts/compare.sh v1-day-02a`.

### Section B — a glance at the pass

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/tuples-and-arrays/destructuring.html">tuples &amp; destructuring</a>
<a href="https://google.github.io/comprehensive-rust/pattern-matching/let-control-flow/let-else.html"><code>let … else</code></a>
<a href="https://google.github.io/comprehensive-rust/iterators/helpers.html">iterator helpers (<code>filter</code>, <code>zip</code>)</a>
<a href="https://doc.rust-lang.org/std/time/struct.Instant.html"><code>Instant</code> &amp; deadlines (std docs)</a>
</div>

Your section A tickets can be copied and read any number of times. But
reading still means standing at the pass for one particular number, for
as long as it takes. Hold three tickets, and you can only `get` them one
by one, in the order you guess. If a dish takes an hour, so does your
`get`. Ray answers this with `ray.wait`: "give me the ones that are
ready — at least this many — but don't keep me longer than this."

Here is the plan in plain words. Lock the pass and count how many of my
numbers have landed. If that's enough, or my time is up, report which
ones. Otherwise, sleep on the bell until something lands *or* the deadline
arrives, whichever comes first, and count again. `Condvar` has a variant
of `wait` for exactly that: `wait_timeout`.

Make three edits to `src/ticket.rs`, in order:

1. Add the clock imports below the other `use std::…` lines (around
   line 7):

   ```rust,ignore
   use std::time::{Duration, Instant};
   ```

2. Inside `impl MemoryStore`, after the closing brace of `get` (around
   line 69), add the waiting method:

   ```rust,ignore
       // ──────── new code starts here (around line 70) ────────
       /// Block until at least `num_ready` of `ids` have outcomes, or until
       /// `timeout` runs out. Returns, for each id in order, whether it is ready.
       pub fn wait(&self, ids: &[ObjectId], num_ready: usize, timeout: Duration) -> Vec<bool> {
           let deadline = Instant::now() + timeout;
           let mut results = self.results.lock().unwrap();
           loop {
               let ready: Vec<bool> = ids.iter().map(|id| results.contains_key(id)).collect();
               let count = ready.iter().filter(|&&is_ready| is_ready).count();
               let now = Instant::now();
               if count >= num_ready || now >= deadline {
                   return ready;
               }
               // Sleep until something lands or the time is up, whichever is first.
               results = self.landed.wait_timeout(results, deadline - now).unwrap().0;
           }
       }
       // ──────── new code ends here ────────
   ```

3. After the closing brace of `impl<T: Clone + 'static> Ticket<T>` and
   before `#[cfg(test)]` (around line 128), add the function you will
   call:

   ```rust,ignore
   // ──────── new code starts here (around line 128) ────────
   /// Analogy: the waiter glances at the pass — for at most `timeout` — and
   /// takes whichever ordered dishes are up, without waiting for all of them.
   ///
   /// Like Ray's `ray.wait`: blocks until `num_ready` of `tickets` have results
   /// (a failed task counts as ready) or `timeout` passes, then splits them into
   /// `(ready, pending)`, each in the original order. All tickets must come from
   /// the same worker.
   pub fn wait<T>(
       tickets: Vec<Ticket<T>>,
       num_ready: usize,
       timeout: Duration,
   ) -> (Vec<Ticket<T>>, Vec<Ticket<T>>) {
       let Some(first) = tickets.first() else {
           return (Vec::new(), Vec::new());
       };
       let num_ready = num_ready.min(tickets.len());
       let ids: Vec<ObjectId> = tickets.iter().map(|ticket| ticket.id).collect();
       let landed = first.store.wait(&ids, num_ready, timeout);

       let mut ready = Vec::new();
       let mut pending = Vec::new();
       for (ticket, is_ready) in tickets.into_iter().zip(landed) {
           // Never hand back more than was asked for, even if more have landed.
           if is_ready && ready.len() < num_ready {
               ready.push(ticket);
           } else {
               pending.push(ticket);
           }
       }
       (ready, pending)
   }
   // ──────── new code ends here ────────
   ```

A few details in those two blocks:

- `filter(|&&is_ready| is_ready)` counts the `true`s. The two `&`s
  unwrap two layers of borrowing: one from iterating the list, and one
  added by `filter`. So `is_ready` is a plain `bool`.

- `wait_timeout` returns a pair: the lock guard, and a note saying
  whether the time ran out. The code keeps the guard (`.0`) and ignores
  the note, because the loop re-checks the clock itself.

- `let Some(first) = tickets.first() else { … }` reads "take the first
  ticket — and if there is none, return two empty lists right away."
  The first ticket also says which pass to look at. That is why every
  ticket must come from the same worker today.

- The final loop walks the tickets and their ready-flags side by side
  (`zip`) and sorts them into two piles. Order is kept, and the `ready`
  pile never grows past `num_ready`, the same rule as Ray's.

Now the demo. Make three edits to `src/main.rs`:

1. Replace the `use std::time::Duration;` and `use worker::Worker;` lines
   with these three:

   ```rust,ignore
   use std::time::{Duration, Instant};
   use ticket::{Ticket, wait};
   use worker::Worker;
   ```

2. Replace `main`'s final closing `}` (around line 47) with these lines,
   which end with a new closing `}`:

   ```rust,ignore
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
   }
   ```

3. Append three small helpers to the end of the file. The last one is
   Day 1's `round_to_100`, back again:

   ```rust,ignore
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
   ```

`cargo run`:

```text
submit slow_square(7) -> ticket #1
clone the ticket      -> ticket #1
get ticket #1         -> 49
get the copy          -> 49
submit a burning task -> ticket #2

thread 'worker-0' (319774978) panicked at src/main.rs:18:5:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get ticket #2 (try 1) -> error: task failed: the oven caught fire
get ticket #2 (try 2) -> error: task failed: the oven caught fire
──── everything above: section A's output, unchanged ────
submit 3 orders       -> tickets [#3, #4, #5]
wait for 1, max 5 s   -> ready [#3], pending [#4, #5] at about 300 ms
wait for 1, max 0.1 s -> ready [], pending [#4, #5] at about 400 ms
wait for 2, max 5 s   -> ready [#4, #5], pending [] at about 900 ms
wait on the burnt one -> ready [#2] (a failure counts as done)
```

(The crash report now says line 18 instead of 17, because the new
`use ticket` line pushed `slow_burn` down by one. Everything else above
the divider is identical.)

Read the new lines one by one:

- `submit 3 orders -> tickets [#3, #4, #5]` — three slips go on the rail
  in one trip, and `numbers` printed their ticket numbers. Each dish takes
  300 ms, and there is one cook, so the dishes will land at about 300, 600
  and 900 ms.

- `wait for 1, max 5 s -> ready [#3] …` — the waiter glances at the pass
  until one dish is up. `wait` returned as soon as #3 landed. The cook's
  `put` rang the bell, and `MemoryStore::wait`'s count reached 1.

- `wait for 1, max 0.1 s -> ready [] …` — this time the waiter gives up
  after 100 ms, because #4 lands at about 600 ms, too late. `wait_timeout` woke
  at the deadline, the clock check returned, and every ticket went to the
  pending pile.

- `wait for 2, max 5 s -> ready [#4, #5] …` — the waiter waits for both
  remaining dishes, and gets them at about 900 ms.

- `wait on the burnt one -> ready [#2]` — section A's burnt dish counts
  as ready. Its spot on the pass holds a note, and a note is an outcome.

The times are rounded to the nearest 100 ms by `round_to_100`. Unrounded,
my run printed 306, 411 and 915 ms: a few milliseconds of thread
wake-up on top of the cooking. Your raw numbers will differ slightly, but
the rounded ones should match.

Five tests pin `wait`. Make two edits to `src/ticket.rs`'s `mod tests`:

1. Replace its import lines (around line 162) with:

   ```rust,ignore
       use super::{MemoryStore, Outcome, Ticket, wait};
       use crate::worker::Worker;
       use std::sync::Arc;
       use std::thread;
       use std::time::{Duration, Instant};
   ```

2. Replace the final closing `}` of the file with:

   ```rust,ignore
       /// Three tickets on one store, none of them ready yet.
       fn three_orders(store: &Arc<MemoryStore>) -> Vec<Ticket<u64>> {
           (0..3)
               .map(|_| Ticket::new(store.new_id(), Arc::clone(store)))
               .collect()
       }

       fn numbers(tickets: &[Ticket<u64>]) -> Vec<String> {
           tickets
               .iter()
               .map(|ticket| ticket.id().to_string())
               .collect()
       }

       #[test]
       fn wait_returns_ready_tickets_in_order_and_at_most_num_ready() {
           let store = MemoryStore::new();
           let orders = three_orders(&store);
           store.put(orders[2].id(), ready(30));
           store.put(orders[0].id(), ready(10));

           let (ready_now, pending) = wait(orders.clone(), 1, Duration::ZERO);
           assert_eq!(numbers(&ready_now), ["#1"]);
           assert_eq!(numbers(&pending), ["#2", "#3"]);

           let (ready_now, pending) = wait(orders, 3, Duration::ZERO);
           assert_eq!(numbers(&ready_now), ["#1", "#3"]);
           assert_eq!(numbers(&pending), ["#2"]);
       }

       #[test]
       fn wait_gives_up_when_the_timeout_runs_out() {
           let store = MemoryStore::new();
           let orders = three_orders(&store);
           let clock = Instant::now();
           let (ready_now, pending) = wait(orders, 1, Duration::from_millis(50));
           assert!(ready_now.is_empty());
           assert_eq!(pending.len(), 3);
           assert!(clock.elapsed() >= Duration::from_millis(50));
       }

       #[test]
       fn wait_wakes_as_soon_as_enough_results_land() {
           let store = MemoryStore::new();
           let orders = three_orders(&store);
           let id = orders[1].id();
           let cook = {
               let store = Arc::clone(&store);
               thread::spawn(move || store.put(id, ready(20)))
           };
           // A generous timeout: the test passes only if `put` wakes the wait.
           let (ready_now, _) = wait(orders, 1, Duration::from_secs(60));
           assert_eq!(numbers(&ready_now), ["#2"]);
           cook.join().unwrap();
       }

       #[test]
       fn a_failed_task_counts_as_ready() {
           let worker = Worker::start("worker-0");
           let burnt = worker.submit(|| -> u64 { panic!("burnt") });
           let (ready_now, _) = wait(vec![burnt], 1, Duration::from_secs(60));
           assert_eq!(ready_now.len(), 1);
           assert!(ready_now[0].get().is_err());
       }

       #[test]
       fn wait_on_no_tickets_returns_at_once() {
           let (ready_now, pending) = wait(Vec::<Ticket<u64>>::new(), 1, Duration::from_secs(60));
           assert!(ready_now.is_empty() && pending.is_empty());
       }
   }
   ```

Look at how they avoid racing the clock:

- `wait_returns_ready_tickets_in_order_and_at_most_num_ready` puts
  results on the pass by hand, then peeks with `Duration::ZERO`, so its
  answer cannot depend on timing.

- `wait_wakes_as_soon_as_enough_results_land` and
  `a_failed_task_counts_as_ready` give `wait` a generous 60-second limit.
  Each normally finishes in a moment, but only if the result's `put`
  really rings the bell. A slow machine only makes them slower, never
  wrong.

- `wait_gives_up_when_the_timeout_runs_out`, the one test that *must*
  involve time, only checks that at least 50 ms passed. A busy laptop
  can make it later, but never earlier.

- `wait_on_no_tickets_returns_at_once` has nothing to wait for at all.

```text
running 17 tests
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::get_returns_the_task_result ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok

test result: ok. 17 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

**Section checkpoint:** `./scripts/compare.sh v1-day-02b`.

### Section C — an order that waits for another dish

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/user-defined-types/enums.html">enums carrying data</a>
<a href="https://doc.rust-lang.org/std/macro.matches.html"><code>matches!</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html"><code>HashMap::entry</code> (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/generics/generic-data.html">generic types (<code>Ticket&lt;A&gt;</code>)</a>
</div>

Your tickets can now be copied, read, and waited on. Can one go *into*
another task? Try the obvious way with what you have. Move a copy into
the next task's closure, and `get` it there. To see what happens when
the input burns, add this to section B's `main`, right above the `wait on
the burnt one` lines:

```rust,ignore
    let ruined = worker.submit(slow_burn);
    let naive = worker.submit(move || ruined.get().unwrap() + 1);
    match naive.get() {
        Ok(value) => println!("naive -> {value}"),
        Err(error) => println!("naive -> error: {error}"),
    }
```

It compiles, and here is the end of what it prints:

```text
thread 'worker-0' (319794911) panicked at src/main.rs:18:5:
the oven caught fire

thread 'worker-0' (319794911) panicked at src/main.rs:65:52:
called `Result::unwrap()` on an `Err` value: TaskError { message: "the oven caught fire" }
naive -> error: task failed: called `Result::unwrap()` on an `Err` value: TaskError { message: "the oven caught fire" }
```

Three things went wrong, and each one gets worse as the kitchen grows:

- **The second task ran anyway.** It started, called `get`, found an
  error, and crashed on `unwrap`. That is a *second* crash report for one
  burnt dish.

- **The error got mangled.** "The oven caught fire" now arrives buried
  inside an `unwrap` complaint. Every task in a longer chain would wrap
  it once more.

- **The cook stood at the pass.** The second task occupied the cook from
  the moment it started until its input landed. With one cook and
  first-in-first-out slips, the input happens to be ready by then, because
  the first task was ahead in line. On Day 4, with several cooks, a cook
  waiting at the pass is a cook not cooking. And if every cook waits for
  a dish that only a cook can make, nothing moves at all.

Delete those lines again. The better design takes the waiting away from
the cook. The slip does not go on the cook's rail yet. Instead, the pass
holds it, clipped to the empty spot of the dish it needs, and the slip
moves to the rail only once that dish has landed. The task itself
receives a plain value. If the input burned, the task never runs, and its
spot on the pass gets a copy of the input's note.

Where does a slip wait in the meantime? Right on the pass, clipped to the
empty spot of the dish it needs. So a spot on the pass now has two
possible states: a finished dish, or a list of slips waiting for one. A
Rust `enum` says exactly that. "Store a result" and "is anyone waiting?"
must be decided together, so both live under the *same* lock. Otherwise
a slip could be clipped a moment after the dish landed, and wait forever.

Make seven edits to `src/ticket.rs`, in order:

1. The pass will hold jobs now, so import `Job` too. Replace the
   `use crate::worker::TaskError;` line (around line 9) with:

   ```rust,ignore
   use crate::worker::{Job, TaskError};
   ```

2. Before the `/// Analogy: the pass` doc comment of `MemoryStore`
   (around line 26), add the spot type:

   ```rust,ignore
   // ──────── new code starts here (around line 26) ────────
   /// Analogy: one numbered spot on the pass — either the dish is there, or
   /// order slips are clipped to the empty spot, waiting for it.
   ///
   /// The store's entry for one id: its outcome, or the jobs to release the
   /// moment that outcome lands.
   enum Slot {
       Ready(Outcome),
       Waiting(Vec<Job>),
   }
   // ──────── new code ends here ────────
   ```

3. In `pub struct MemoryStore`, the table now maps ids to spots. Change
   the `results` field (around line 42) to:

   ```rust,ignore
       results: Mutex<HashMap<ObjectId, Slot>>,
   ```

4. Replace the whole `put` method (around line 62) with this new `put`
   followed by the new `when_ready`:

   ```rust,ignore
       // ──────── replacement starts here (around line 62) ────────
       /// Record the outcome for `id`, wake everyone waiting, and release the
       /// jobs that were held for it.
       pub fn put(&self, id: ObjectId, outcome: Outcome) {
           let before = self
               .results
               .lock()
               .unwrap()
               .insert(id, Slot::Ready(outcome));
           self.landed.notify_all();
           // The lock is already released here, so a released job may use the store.
           if let Some(Slot::Waiting(held)) = before {
               for job in held {
                   job();
               }
           }
       }

       /// Run `job` once `id` has an outcome: now, if it already has one;
       /// otherwise later, from inside the `put` that stores it.
       pub fn when_ready(&self, id: ObjectId, job: Job) {
           let mut results = self.results.lock().unwrap();
           let slot = results
               .entry(id)
               .or_insert_with(|| Slot::Waiting(Vec::new()));
           if let Slot::Waiting(held) = slot {
               held.push(job);
               return;
           }
           drop(results);
           job();
       }
       // ──────── replacement ends here ────────
   ```

5. In `get` (around line 98), a spot may now hold slips instead of a
   dish, so only a `Ready` spot counts. Change its `if let` line to:

   ```rust,ignore
               if let Some(Slot::Ready(outcome)) = results.get(&id) {
   ```

6. The same in `wait` (around line 112). Replace the `let ready: Vec<bool>
   = …` line with:

   ```rust,ignore
               let ready: Vec<bool> = ids
                   .iter()
                   .map(|id| matches!(results.get(id), Some(Slot::Ready(_))))
                   .collect();
   ```

   `matches!(value, pattern)` is `true` when the value fits the pattern.
   The `_` inside `Slot::Ready(_)` means "anything in there."

7. Give `Ticket` its own `when_ready`, so code holding only a ticket can
   ask the pass to hold a job for it. Inside `impl<T> Ticket<T>`, after the
   closing brace of `id` (around line 153), add:

   ```rust,ignore
       // ──────── new code starts here (around line 154) ────────
       /// Run `job` once this ticket's result (or error) has landed.
       pub fn when_ready(&self, job: Job) {
           self.store.when_ready(self.id, job);
       }
       // ──────── new code ends here ────────
   ```

Two details carry the whole design:

- In `put`, `insert` hands back whatever the spot held *before*. If it
  was `Waiting`, those are the held slips, and `put` releases them. But it
  does so only after the lock is gone. Here is why the lock is gone. The
  guard that `lock()` returns is never stored in a variable. It is used
  once, for `insert`, inside the `let before = …` statement. A value nobody
  stores is dropped as soon as its statement ends, and dropping the guard
  opens the lock. So by the time the release loop runs, the pass is
  unlocked again. Why bother? A released job is arbitrary code. Today's
  job only sends a slip down a channel. But a job that touched the pass
  itself would try to take a lock its own thread already holds, and a
  `Mutex` answers that by deadlocking (or panicking). Releasing outside the
  lock rules that out for good.

- `when_ready` uses `entry(id).or_insert_with(…)`: "find id's spot,
  creating an empty `Waiting` one if there is none." If the spot is
  `Waiting`, `when_ready` clips the job on and returns. If it is `Ready`,
  the dish is already there, so `when_ready` unlocks (`drop(results)`) and
  runs the job right away.

Now the worker's side, in `src/worker.rs`. Inside `impl Worker`, after
the closing brace of `submit` (around line 61), add `submit_with`:

```rust,ignore
    // ──────── new code starts here (around line 62) ────────
    /// Like `submit`, but `task` takes the result behind `input` as its
    /// argument. Returns a ticket at once; the task joins the queue only
    /// once `input` has landed. If `input` failed, `task` never runs and its
    /// ticket carries the same error.
    pub fn submit_with<A, T, F>(&self, input: &Ticket<A>, task: F) -> Ticket<T>
    where
        A: Clone + Send + Sync + 'static,
        F: FnOnce(A) -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let id = self.store.new_id();
        let promise = Promise::new(id, Arc::clone(&self.store));
        let input_copy = input.clone();
        let job: Job = Box::new(move || {
            // Already landed by the time this job runs, so `get` won't block.
            let outcome = match input_copy.get() {
                Ok(value) => run_catching(move || task(value)),
                Err(error) => Err(error),
            };
            promise.keep(outcome);
        });
        let sender = self.sender.clone().expect("worker is running");
        input.when_ready(Box::new(move || {
            // As in `execute`: a job sent to a stopped worker is dropped unrun.
            let _ = sender.send(job);
        }));
        Ticket::new(id, Arc::clone(&self.store))
    }
    // ──────── new code ends here ────────
```

Read it as two pieces that meet later:

- `job` is the slip. It gets the input, which has landed by the time the
  job runs, so `get` returns at once. It runs the task on the input with
  `run_catching`. If the input is an error, it skips the task and passes
  the error on. Then it keeps the stub. The slip is built now, but not
  pinned to the rail.

- The small closure handed to `input.when_ready` is the clip. When the
  input lands, the clip pins `job` to the rail by sending it down a copy
  of the worker's channel `sender`. Until then, it waits on the input's spot.

`submit_with` returns the new ticket immediately, whether or not the
input has landed yet. The generic letters read like `submit`'s. `A` is
the input's type, `T` the result's, and `F` the task: a function that
takes an `A` and returns a `T`.

Now feed tickets into tasks. Make two edits to `src/main.rs`:

1. After `slow_square` (around line 14), add three plain one-argument
   functions. They print, so you can see which tasks the cook runs, and
   in what order:

   ```rust,ignore
   // ──────── new code starts here (around line 15) ────────
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
   // ──────── new code ends here ────────
   ```

2. Replace `main`'s final closing `}` (around line 86) with these lines,
   which end with a new closing `}`:

   ```rust,ignore
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
   ```

`submit_with(&base, add_one)` passes `add_one` by name, the same way
section A passed `slow_burn`. A plain function that takes a `u64` is
already a task that takes a `u64`.

`cargo run`:

```text
submit slow_square(7) -> ticket #1
clone the ticket      -> ticket #1
get ticket #1         -> 49
get the copy          -> 49
submit a burning task -> ticket #2

thread 'worker-0' (319781764) panicked at src/main.rs:35:5:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get ticket #2 (try 1) -> error: task failed: the oven caught fire
get ticket #2 (try 2) -> error: task failed: the oven caught fire
submit 3 orders       -> tickets [#3, #4, #5]
wait for 1, max 5 s   -> ready [#3], pending [#4, #5] at about 300 ms
wait for 1, max 0.1 s -> ready [], pending [#4, #5] at about 400 ms
wait for 2, max 5 s   -> ready [#4, #5], pending [] at about 900 ms
wait on the burnt one -> ready [#2] (a failure counts as done)
──── everything above: sections A and B's output, unchanged ────
submit slow_square(3) -> ticket #6
submit add_one(#6)    -> ticket #7
submit double(#7)     -> ticket #8
submit times_ten(#6)  -> ticket #9
is #6 ready yet?      -> false
    cook: add_one(9) = 10
    cook: times_ten(9) = 90
    cook: double(10) = 20
get ticket #8         -> 20
get ticket #9         -> 90
submit slow_burn      -> ticket #10
submit add_one(#10)   -> ticket #11

thread 'worker-0' (319781764) panicked at src/main.rs:35:5:
the oven caught fire
get ticket #11        -> error: task failed: the oven caught fire
```

(One line above the divider changed: the crash report now says line 35
instead of 18. The three new functions, with their doc comment, pushed
`slow_burn` down 17 lines.)

Read the new lines one by one:

- `submit slow_square(3) -> ticket #6` — an ordinary slip, on the rail.
  The cook starts its 300 ms of cooking.

- `submit add_one(#6) -> ticket #7` — a slip that needs dish #6. It gets
  clipped to the empty spot #6, and the waiter gets ticket #7 at once.
  `submit_with` built the job and handed the clip to `when_ready`, which
  found no dish on spot #6 and clipped it there.

- `submit double(#7) -> ticket #8` — a slip that needs a dish whose own
  slip is still waiting: a chain of three. It is clipped to spot #7.

- `submit times_ten(#6) -> ticket #9` — a second slip needing dish #6.
  The same ticket is used twice, the move that could not compile at the
  start of section A.

- `is #6 ready yet? -> false` — a peek (`wait` with `Duration::ZERO`)
  proves that all three dependent slips were placed before their input
  existed.

- `cook: add_one(9) = 10` and `cook: times_ten(9) = 90` — dish #6 (3 × 3
  = 9) lands. `put` rings the bell and releases both slips clipped to its
  spot, in the order they were clipped. Each receives the plain value 9.

- `cook: double(10) = 20` — `double` runs last, although it was
  submitted before `times_ten`. Its slip waited on spot #7, so it reached
  the rail only when `add_one`'s dish landed. By then `times_ten` was
  already on the rail, ahead of it.

- `get ticket #8 -> 20` and `get ticket #9 -> 90` — the waiter collects
  the ends of both chains.

- `submit slow_burn -> ticket #10`, `submit add_one(#10) -> ticket #11` —
  a slip that needs a dish which is about to burn.

- The crash report, then `get ticket #11 -> error: task failed: the oven
  caught fire` — dish #10 burns. Its note lands on spot #10 and releases
  slip #11. That job found an error where its input should be, so it
  skipped `add_one` entirely: there is no `cook: add_one(…)` line, and
  only *one* crash report. It kept its stub with the same note, message
  intact.

Seven tests pin the new behavior. In `src/ticket.rs`:

1. In `mod tests`, change `use std::sync::Arc;` (around line 209) to
   `use std::sync::{Arc, mpsc};`.

2. Replace the final closing `}` of the file with:

   ```rust,ignore
       #[test]
       fn when_ready_holds_a_job_until_the_result_lands() {
           let store = MemoryStore::new();
           let id = store.new_id();
           let (tell, heard) = mpsc::channel();
           store.when_ready(id, Box::new(move || tell.send("released").unwrap()));
           assert!(heard.try_recv().is_err(), "held while nothing has landed");
           store.put(id, ready(1));
           assert_eq!(heard.try_recv(), Ok("released"));
       }

       #[test]
       fn when_ready_runs_at_once_if_the_result_is_already_there() {
           let store = MemoryStore::new();
           let id = store.new_id();
           store.put(id, ready(1));
           let (tell, heard) = mpsc::channel();
           store.when_ready(id, Box::new(move || tell.send("ran").unwrap()));
           assert_eq!(heard.try_recv(), Ok("ran"));
       }
   }
   ```

And in `src/worker.rs`, replace the final closing `}` of the file with:

```rust,ignore
    #[test]
    fn submit_with_returns_before_its_input_is_ready() {
        let worker = Worker::start("worker-0");
        let (open_gate, gate) = mpsc::channel::<()>();
        let first = worker.submit(move || {
            gate.recv().unwrap();
            20_u64
        });
        // The input cannot be ready (the gate is shut), yet we get a ticket.
        let second = worker.submit_with(&first, |x| x + 1);
        open_gate.send(()).unwrap();
        assert_eq!(second.get().unwrap(), 21);
    }

    #[test]
    fn one_ticket_can_feed_several_tasks_and_chains() {
        let worker = Worker::start("worker-0");
        let base = worker.submit(|| 3_u64);
        let plus_one = worker.submit_with(&base, |x| x + 1);
        let doubled = worker.submit_with(&plus_one, |x| x * 2);
        let times_ten = worker.submit_with(&base, |x| x * 10);
        assert_eq!(doubled.get().unwrap(), 8);
        assert_eq!(times_ten.get().unwrap(), 30);
        assert_eq!(base.get().unwrap(), 3);
    }

    #[test]
    fn a_held_task_joins_the_queue_when_its_input_lands() {
        let worker = Worker::start("worker-0");
        let order = Arc::new(Mutex::new(Vec::new()));
        let (open_gate, gate) = mpsc::channel::<()>();
        let log = Arc::clone(&order);
        let first = worker.submit(move || {
            gate.recv().unwrap();
            log.lock().unwrap().push("first");
        });
        let log = Arc::clone(&order);
        let held = worker.submit_with(&first, move |()| log.lock().unwrap().push("held"));
        let log = Arc::clone(&order);
        let later = worker.submit(move || log.lock().unwrap().push("later"));
        open_gate.send(()).unwrap();
        held.get().unwrap();
        later.get().unwrap();
        // "later" was submitted after "held" but queued first: "held" waited
        // off the queue until "first" landed.
        assert_eq!(*order.lock().unwrap(), ["first", "later", "held"]);
    }

    #[test]
    fn a_failed_input_skips_the_task_and_passes_the_error_on() {
        let worker = Worker::start("worker-0");
        let ran = Arc::new(AtomicBool::new(false));
        let burnt = worker.submit(|| -> u64 { panic!("the oven caught fire") });
        let flag = Arc::clone(&ran);
        let next = worker.submit_with(&burnt, move |x| {
            flag.store(true, Ordering::SeqCst);
            x + 1
        });
        let after_that = worker.submit_with(&next, |x| x * 2);
        assert_eq!(
            after_that.get().unwrap_err().to_string(),
            "task failed: the oven caught fire"
        );
        assert!(
            !ran.load(Ordering::SeqCst),
            "the task after a burnt input never runs"
        );
    }

    #[test]
    fn a_panic_in_the_second_task_stays_on_its_own_ticket() {
        let worker = Worker::start("worker-0");
        let base = worker.submit(|| 7_u64);
        let bad = worker.submit_with(&base, |x| -> u64 { panic!("table {x} sent it back") });
        assert_eq!(
            bad.get().unwrap_err().to_string(),
            "task failed: table 7 sent it back"
        );
        assert_eq!(base.get().unwrap(), 7);
    }
}
```

None of them sleeps or measures time. The two `when_ready` tests drive
the pass by hand, with a `put` or without one. Two worker tests use a
*gate*, a channel only the test can open, to keep a task provably
unfinished. They are `submit_with_returns_before_its_input_is_ready` and
`a_held_task_joins_the_queue_when_its_input_lands`. The second pins the
ordering you saw in the demo: a slip submitted later, but not waiting on
anything, runs before a held slip. The other three only block in `get`,
which returns when the result lands, however fast or slow your machine
is.

```bash
cargo test
```

```text
running 24 tests
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

All 24 pass: section B's 17, plus section C's 7.

**Section checkpoint:** `./scripts/compare.sh v1-day-02c` — which is also
the finished day.

## 7. Run it

```bash
cargo run
```

## 8. Expected result

```text
submit slow_square(7) -> ticket #1
clone the ticket      -> ticket #1
get ticket #1         -> 49
get the copy          -> 49
submit a burning task -> ticket #2

thread 'worker-0' (319781764) panicked at src/main.rs:35:5:
the oven caught fire
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
get ticket #2 (try 1) -> error: task failed: the oven caught fire
get ticket #2 (try 2) -> error: task failed: the oven caught fire
submit 3 orders       -> tickets [#3, #4, #5]
wait for 1, max 5 s   -> ready [#3], pending [#4, #5] at about 300 ms
wait for 1, max 0.1 s -> ready [], pending [#4, #5] at about 400 ms
wait for 2, max 5 s   -> ready [#4, #5], pending [] at about 900 ms
wait on the burnt one -> ready [#2] (a failure counts as done)
submit slow_square(3) -> ticket #6
submit add_one(#6)    -> ticket #7
submit double(#7)     -> ticket #8
submit times_ten(#6)  -> ticket #9
is #6 ready yet?      -> false
    cook: add_one(9) = 10
    cook: times_ten(9) = 90
    cook: double(10) = 20
get ticket #8         -> 20
get ticket #9         -> 90
submit slow_burn      -> ticket #10
submit add_one(#10)   -> ticket #11

thread 'worker-0' (319781764) panicked at src/main.rs:35:5:
the oven caught fire
get ticket #11        -> error: task failed: the oven caught fire
```

The whole evening at the pass, one stretch at a time:

- `ticket #1` through `get the copy -> 49` — the dish lands on spot #1
  and stays there. The ticket and its photocopy both read it.
  (`MemoryStore::get`, through `Ticket::get`.)

- `ticket #2` and its two `try` lines — a burnt-dish note on spot #2,
  read twice. (`run_catching` caught the crash, and the stub's `keep`
  stored it.)

- `submit 3 orders` and the `wait` lines — the waiter glances at the pass
  with a time limit. One dish is up, then none in time, then both of the
  rest. A burnt dish counts as done. (`wait`, over `MemoryStore::wait`.)

- `ticket #6` through `get ticket #9` — slips clipped to empty spots, a
  chain and a fan-out, each released the moment its dish lands, each fed
  a plain value. (`submit_with`, `when_ready`, and the release loop in
  `put`.)

- `ticket #10`, `ticket #11` — the burnt note travels down the chain, and
  the dependent task never runs.

A few things vary between runs. The thread id in brackets varies, and so
does the panic's line number if your `main` is laid out differently. In
principle the timings vary too, but the rounding hides that. Everything else, including the order
of the `cook:` lines, is the same every run. (One honest caveat: the demo
relies on the 300 ms of cooking to print its `submit` lines before the
cook's lines. The tests don't rely on timing at all.)

Two more things to notice:

- Only one crash report prints for the second burnt dish, although two
  tickets carry its note. The note is *copied* onto spot #11, and nothing
  burned twice.

- The `note: run with RUST_BACKTRACE=1` line appears only once, after the
  first crash. Rust prints that hint only for the first panic in a
  program.

## 9. Test it

Twenty-four tests now pin the engine: Day 1's seven, plus 17 new ones
today. Section A's 5 pin numbered, copyable tickets and the stub that
prevents endless waits. Section B's 5 pin `wait` with order, limits and
timeouts. Section C's 7 pin held slips, chains, fan-out and error
propagation:

```bash
cargo test
```

```text
running 24 tests
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 24 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

(All 24 are unit tests. The program takes no command-line input yet, so
there is nothing for a command-line test to drive.)

## 10. Break it deliberately

Silence the bell. In `MemoryStore::put` in `src/ticket.rs`, delete one
line:

```diff
-        self.landed.notify_all();
```

Everything still compiles, because the compiler cannot know that a
sleeping thread needs waking. `cargo run` prints two lines and then **hangs**
(press Ctrl-C to stop it):

```text
submit slow_square(7) -> ticket #1
clone the ticket      -> ticket #1
```

Read the silence:

- `get ticket #1` never prints. `MemoryStore::get` found spot #1 empty
  and went to sleep on the bell. The cook put 49 on the pass about 300 ms
  later, but never rang. So the waiter sleeps on, beside a finished dish.

`cargo test` is more talkative. Ten tests pass at once. Then, after a
minute, the test runner starts reporting the others (lines trimmed to
the interesting ones):

```text
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok
test ticket::tests::a_failed_task_counts_as_ready has been running for over 60 seconds
test ticket::tests::get_blocks_until_another_thread_puts_the_result has been running for over 60 seconds
test ticket::tests::wait_wakes_as_soon_as_enough_results_land has been running for over 60 seconds
test worker::tests::get_returns_the_task_result has been running for over 60 seconds
test worker::tests::one_ticket_can_feed_several_tasks_and_chains has been running for over 60 seconds
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
```

Fourteen tests were reported as running for over a minute, and the five
shown are a sample. Read it as a post-mortem:

- The ten that passed never needed the bell. They put the result on the
  pass *before* anyone waited for it (like
  `every_copy_of_a_ticket_reads_the_same_result`), or never waited at all.

- `get_returns_the_task_result` and the other `get`-based tests hang
  forever, exactly like the demo. I stopped the run after 70 seconds.

- `wait_wakes_as_soon_as_enough_results_land` and
  `a_failed_task_counts_as_ready` *did* pass — after 60 seconds, which is
  their timeout. `wait_timeout` woke at the deadline, re-counted, and
  found the dish that had been sitting there for a minute. That is why
  the wake-up test gives `wait` a generous 60 s limit and then expects to
  finish fast. A missing bell turns "fast" into "a minute," and a minute
  is impossible to miss.

The lesson: **a condition variable comes with a duty to notify.** Every place
that changes what sleepers are waiting for must ring the bell, or the
sleepers sleep through it. Ray's own object store has the same duty.
Whenever an object becomes available, everyone blocked on it must be told.

Put the `notify_all` line back and confirm `cargo test` is green again.

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
./scripts/compare.sh v1-day-02
```

**Inspecting instead of typing?** Jump to today's exact finished state,
poke at it, and come back:

```bash
git switch --detach v1-day-02
cargo test
git switch -          # back to where you were
```

**Either way**, today's complete change set — docs included — reads as one
diff:

```bash
git diff v1-day-01 v1-day-02
```

## 12. Recap

**What changed today:** results moved out of the tickets' private
channels onto the pass. The pass is a `MemoryStore` table keyed by
`ObjectId`, guarded by a `Mutex` and a `Condvar` bell. Tickets became
copyable ids that `get` can read any number of times. The cook's stub (`Promise`) guarantees every
ticket's spot is eventually filled, even if a job is thrown away. `wait`
collects whichever results are ready, with a time limit, in Ray's
`(ready, pending)` shape. `submit_with` passes a ticket into the next
task. The slip waits clipped to its input's spot, joins the rail when the
input lands, and receives a plain value. If the input failed, the slip
never runs, and it inherits the input's error instead. Seventeen new tests (24 in all) pin it
across three section checkpoints (`v1-day-02a`, `v1-day-02b`,
`v1-day-02c`), still with no dependencies.

**Deliberately naive:** `submit_with` takes exactly one input ticket,
where Ray accepts any number. `wait` needs all its tickets to be of one
type and from one worker. There is still one cook, so held slips never
save time yet. Results are never removed from the pass, so a long-running
program's pass only grows. And every result lives in the memory of this
one program. A dish is shared by pointer, but there is no place that
several workers could read the same bytes from.

**Tomorrow's limitation, resolved on Day 3:** the pass is a table inside
one program, holding Rust values. On Day 3 the kitchen gets a
**pantry**, an object store. Results are put in once, and every worker
reads them where they stand, without copying. That piece lets results outlive
the task that made them. Later, it lets them cross the line between
processes and machines. The scene grows from a counter by the
stove into a shared pantry — see [the restaurant](story.md).
