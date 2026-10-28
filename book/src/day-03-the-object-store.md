# Day 3: The object store

> **Day 3 of 17** · builds on `v1-day-02` · ≈ 2 h

| Section | Builds | Checkpoint |
|---------|--------|------------|
| A | Read the pot where it stands: sharing instead of copying | `v1-day-03a` |
| B | One pass for the whole kitchen | `v1-day-03b` |
| C | The waiter stocks the pantry: `put` | `v1-day-03c` = `v1-day-03` |

**Pick your mode before starting** (explained fully in
[Setup](setup.md#4-choose-how-you-follow-along)):

```bash
# Build mode — start (or restart) your branch from yesterday's checkpoint.
# Already on my-course from Day 2? Keep going — no command needed.
git switch -c my-course v1-day-02

# Inspect mode — read along with the finished code instead:
git switch --detach v1-day-03a         # then 03b, 03c as you pass each section
```

<div class="callout">

**Today's typing budget** — so nothing sneaks up on you:

| Section | The learning | Wiring & tests (copy-friendly) |
|---------|--------------|--------------------------------|
| A | `Ticket::read`, a hand-written `Clone`, `submit_reading` · ~60 lines; `get` and `submit_with` shrink to a few lines each | `src/meter.rs` (~30 lines, copy it), a new `main`, 5 tests |
| B | `Worker::start_in` · ~15 lines | one demo function + 2 helpers, 3 tests |
| C | `put` · ~10 lines | one demo function, 5 tests |

Section A is the longest sitting of the day, and sections B and C are short.
Three sections are three natural sittings, and every checkpoint is a save
point — nothing says a "day" must fit in one. Still **no dependencies**:
everything comes from Rust's standard library.

</div>

## 1. What you will have running

Yesterday a result could be read many times, through any copy of its
ticket. But each time a task or your program collected a result, it got
its own *copy* of it. For a number, a copy costs nothing. But for a big
value, such as an image, a table or a model's weights, copying is most of
the work. By the end of today, `cargo run` does four new things:

- **measures the copying.** The program counts every byte of fresh memory
  it asks for. Eight small tasks that each need one 64 MiB value make
  512 MiB of copies.

- **shares instead of copying.** The same eight tasks read the one stored
  value where it sits. They need no fresh memory at all, and every task
  sees the very same bytes.

- **lets several workers share one store,** so their results get one
  numbering, and each worker can read the others' results without a copy.

- **stores a value you already have**, with no task, no worker and no
  waiting, and hands back a ticket for it on the spot.

That is Ray's object store in miniature: **put a value in once, and
everyone reads that one copy.**

<div class="dl-concepts"><span>Ray concepts used here:</span>
<a href="https://docs.ray.io/en/latest/ray-core/objects.html">objects ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/api/doc/ray.put.html">ray.put ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/objects/serialization.html">the object store: immutable, shared memory ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/patterns/pass-large-arg-by-value.html">anti-pattern: a large argument passed by value ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/patterns/closure-capture-large-objects.html">anti-pattern: closures capturing large objects ↗</a>
</div>

## 2. What you will be seeing in the output

A sneak peek at the end of the day — the exact output you will produce,
with a plain-English note under each group of lines. If a note doesn't
fully land yet, that's fine. The rest of the day builds every piece:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 3.8 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 10.8 ms, 512.0 MiB allocated
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
8 tasks on 2 cooks        -> 8 done: 303.5 ms, 0.0 MiB allocated
saw the delivery's bytes  -> 8 of 8
```

- The first line — a job builds one big value, 64 MiB of bytes, and the
  value is stored under number `#1`.

- `get #1` — collecting that value the old way hands over a full copy.
  That is 64 MiB of fresh memory, in about 4 ms.

- `8 tasks, submit_with` — eight jobs each need the value, and each gets
  its own copy: 8 × 64 = 512 MiB. The work itself is tiny, so the copying
  is the whole cost.

- `read #1` and `8 tasks, submit_reading` — the new way looks at the one
  stored value instead of copying it. It uses no fresh memory, and no time
  worth measuring.

- `saw #1's own bytes` — proof by address. Each job reported *where in
  memory* the bytes it looked at live. None of the eight copies sit at the
  original's address, but all eight readers do.

- The next five lines — two workers now share one store. Their results
  get one numbering (`#1`, `#2`, `#3`, never two `#1`s). Waiting for both
  results returns when the slow one is done, about a second later. And the
  second worker reads the first one's 64 MiB without a copy.

- `both cooks busy` down to the last line — the program hands its *own*
  64 MiB value to the store. The value is stored and ready at once, even
  though both workers are still busy for 300 ms. No worker was needed, and
  not a byte was copied. When the workers free up, eight jobs on the two
  of them read it, and all eight see the very bytes the program made.

## 3. Why we need it

**The story so far — remember [the restaurant](story.md):** one
[cook](story.md#worker) works through [order slips](story.md#task). Each
finished dish lands on [the pass](story.md#the-pass) under its
[ticket number](story.md#ticket-number), and anyone holding a copy of the
number can collect it. A [held slip](story.md#held-slip) waits clipped to
the spot of the dish it needs, and goes to the cook once that dish lands.

Today's scene: **the pantry.** Three things about yesterday's pass stop
working the moment the dishes get big or the kitchen gets busy:

- **Every collection is a fresh ladle.** Collecting a result with
  [`get`](story.md#get) ladles a full copy into a new pot for you, and
  every held slip gets its own pot too. For a bowl of soup that's fine.
  But take a huge pot of stock that eight orders each need a little of.
  The kitchen fills eight new pots, and you will measure exactly that in a
  minute.

- **Every cook brings their own pass.** Hire a second cook, and they
  bring a second counter with its own numbering. Two different dishes can both
  be "#1", and [a glance](story.md#wait) at one counter never sees the
  other's dishes.

- **The waiter's own ingredients have no way in.** Take something the
  waiter already holds, like a delivery that just arrived. It can reach the
  orders only by being copied into every slip, or by being handed to a cook
  as a pretend order and waiting its turn in line.

Section A lets every task, and your program, look into the pot where it
stands instead of ladling out a copy. Section B gives the whole kitchen
one pass. Section C lets the waiter stock the pantry directly.

## 4. Mental model

Walk the kitchen, one station per section:

- **The pantry (section A).** Look closer at a spot on the pass: it never
  held the result itself. Since Day 2, every finished result has sat, as
  one pot, in exactly one place. Call that place the pantry. The result's
  spot on the pass holds only a *label* saying where the pot stands.
  Collecting with `get` was a ladle: the kitchen filled a brand-new pot for
  you and handed it over. Today's `read` hands you another label instead. Any number of labels can point at the same
  pot, and the pot stays until the last label is gone.

- **One pass for the whole kitchen (section B).** The pass belongs to the
  kitchen, not to one cook. Every cook files results on it, and every pot
  gets a number nobody else has. A glance at the pass sees them all.

- **The waiter stocks the pantry (section C).** The waiter carries a
  delivery straight into the pantry and gets a ticket for it on the spot,
  with no slip, no cook and no queue. And everything in the pantry is
  **sealed**: readers look, and nobody changes it. That rule makes sharing
  safe. Any number of cooks can read a pot that never changes at once,
  with nobody waiting for anybody.

```text
the stock, made once                64 MiB in the pantry; its spot holds a label
   │ get, or a submit_with task
   ▼
a fresh pot for each reader         another 64 MiB every time (8 readers: 512 MiB)


the stock, made once                the same 64 MiB, untouched
   │ read, or a submit_reading task
   ▼
one more label for each reader      a pointer to the same pot (8 readers: 0 MiB)
```

Read each flow top to bottom. The left column is what happens to the
stock, and the right column says what it costs. The two flows start from
the same pot, but only the first one ever fills a second.

## 5. Ray concepts introduced

- **The object store.** Each Ray node runs one object store: an area of
  shared memory that every worker process on that machine can see.
  Objects in it are immutable, so many workers can read the same object
  at once. In this book, Ray's object store is both halves of one
  kitchen: **the pantry**, where values live, and **the pass**
  (`MemoryStore`), the numbered table of labels pointing into it. Two
  names, two parts — never two names for one part.

- **Zero-copy reads.** In Ray, `ray.get` of a NumPy array returns an array
  *backed by the object store's memory*, with no copy, and read-only.
  Today's `Ticket::read` does the same. It returns an `Arc<T>`, a shared
  pointer to the one stored value, and through it you can only read.

- **`ray.put(value)`** stores a value the program already has and returns
  an `ObjectRef` for it at once. Today's `put(&store, value)` is the same.
  Results and put objects share one store and one numbering. A ticket is
  a ticket, wherever the value came from.

- **Pass the ref, not the value.** Ray's docs list two anti-patterns:
  passing the same large argument by value to many tasks, and closures
  that capture a large object. Both copy the value once per task. The fix
  for both is to `put` it once and hand the tasks its ref. That is exactly
  what you measure today.

- **One honest difference.** Ray's object store holds *bytes*, because
  Ray's workers are separate processes that cannot share a Rust (or
  Python) value directly. Today's workers are threads in one process, so
  their memory is already shared, and the pantry can hold the values
  themselves. On Day 6 values become bytes, so that on Day 7 they can
  cross to another machine.

## 6. Build it, section by section

### Section A — read the pot where it stands

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/user-defined-types/const.html"><code>const</code></a>
<a href="https://google.github.io/comprehensive-rust/user-defined-types/static.html"><code>static</code></a>
<a href="https://google.github.io/comprehensive-rust/unsafe-rust/unsafe.html"><code>unsafe</code></a>
<a href="https://google.github.io/comprehensive-rust/unsafe-rust/unsafe-traits.html">unsafe traits</a>
<a href="https://doc.rust-lang.org/std/alloc/trait.GlobalAlloc.html"><code>GlobalAlloc</code> (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/std-traits/casting.html">casting with <code>as</code></a>
<a href="https://google.github.io/comprehensive-rust/memory-management/clone.html"><code>Clone</code></a>
<a href="https://doc.rust-lang.org/std/sync/struct.Arc.html#method.downcast"><code>Arc::downcast</code> (std docs)</a>
<a href="https://google.github.io/comprehensive-rust/references/shared.html">shared references (<code>&amp;T</code>)</a>
<a href="https://google.github.io/comprehensive-rust/unsafe-rust/dereferencing.html">raw pointers (<code>*mut u8</code>)</a>
<a href="https://doc.rust-lang.org/book/ch19-03-pattern-syntax.html#extra-conditionals-with-match-guards">match guards (<code>if</code> in a pattern, Rust book)</a>
<a href="https://doc.rust-lang.org/reference/expressions/call-expr.html#disambiguating-function-calls">fully qualified calls (<code>T::clone(…)</code>, reference)</a>
<a href="https://doc.rust-lang.org/book/ch15-02-deref.html#implicit-deref-coercions-with-functions-and-methods">deref coercion (<code>&amp;Arc&lt;A&gt;</code> → <code>&amp;A</code>, Rust book)</a>
</div>

Your Day 2 pass already keeps each result exactly once, so why touch it?
Because of what happens when someone *collects* a result. You cannot see
memory being copied. So before changing anything, build a meter that
shows it.

Every `Vec`, `String`, or `Box` your program makes asks one component
for its memory: the *allocator*. Rust lets a program swap in its own.
Yours will pass every request straight to the system's allocator, after
adding the request's size to a running total. This file is a measuring
tool, not today's lesson, so type it or copy it. It has to compile, and it
exists so the demo can measure, not to test you. Two things in it deserve
a sentence each:

- `#[global_allocator]` marks the one value that serves every allocation
  in the program, on every thread.

- `unsafe` marks code where the compiler cannot check the rules for you.
  Handing out raw memory is such code, so the trait and its methods are
  `unsafe`. Yours is sound for a plain reason: it never touches the
  memory. It only forwards each call and counts.

Create `src/meter.rs`:

```rust,ignore
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Analogy: a meter on the kitchen's supply line — it counts every byte of
/// fresh memory the program asks for, whichever thread asks.
///
/// A global allocator that hands every request to the system allocator and
/// adds its size to a running total.
struct Meter;

#[global_allocator]
static METER: Meter = Meter;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every request goes straight to the system allocator; we only count.
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Total bytes allocated since the program started. It never goes down:
/// freeing memory does not undo the work of having made it.
pub fn allocated() -> usize {
    ALLOCATED.load(Ordering::Relaxed)
}
```

`ALLOCATED` is a `static`: one value that lives as long as the program,
shared by every thread. That is why it is an atomic, like Day 2's
`next_id`. And `*mut u8` is a *raw pointer*: a bare memory address that
the compiler does not track. That is exactly what an allocator hands out.

Now a demo that measures. Replace `src/main.rs` with the version below.
It builds one 64 MiB value with a task, collects it with `get`, then
feeds it to eight tasks with Day 2's `submit_with`. It prints the time and
the fresh memory each step cost:

```rust,ignore
mod meter;
mod ticket;
mod worker;

use std::time::{Duration, Instant};
use ticket::{Ticket, wait};
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
```

A few details:

- `const STOCK_BYTES` is a named constant, fixed when the program is
  compiled. `vec![7; STOCK_BYTES]` makes a vector of that many bytes, all
  equal to 7.

- Each task returns `address(&bytes)`: *where* in memory the bytes it
  looked at live. `bytes.as_ptr()` is that location, and `as usize` turns
  it into a plain number you can compare. Two equal addresses mean the
  very same bytes, and a copy always lives somewhere else.

- `start_measuring` notes the meter and the clock, and `cost` reports how
  much of each passed since. The two numbers belong together, so they travel
  as one pair. In `format!("{ms:.1} ms, …")`, the `:.1` prints one digit
  after the decimal point.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 4.9 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 11.7 ms, 512.0 MiB allocated
```

Read it line by line:

- `submit make_stock -> ticket #1, ready: true` — the cook makes the pot
  of stock. `wait` with a 60 s limit held the waiter until the pot landed,
  so making it is not counted below.

- `get #1 -> 64 MiB of bytes: … 64.0 MiB allocated` — collecting it once
  filled a whole new 64 MiB pot. `Ticket::get` did that.

- `8 tasks, submit_with -> 8 done: … 512.0 MiB allocated` — eight slips
  filled eight fresh pots. Each task only reads an address, yet every one
  of them first got a full private copy.

Where does the copy happen? Not on the pass. Look at Day 2's `get`. The
pass handed it an `Arc`, a shared pointer to the one stored value. Then
the last line did `value.clone()`. That clone is the ladle. And Day 2's
`submit_with` called `get` once for every task it fed.

So the fix is small: hand out the shared pointer itself. Cloning an `Arc`
copies a pointer and bumps a count, and the value stays where it is. Make
two edits to `src/ticket.rs`, in order:

1. Delete the `#[derive(Clone)]` line above `pub struct Ticket<T>`
   (around line 132). You will write `Clone` by hand in the next step.

2. Replace the whole `impl<T: Clone + 'static> Ticket<T>` block (around
   line 159), which holds `get`, with these two blocks:

   ```rust,ignore
   // ──────── replacement starts here (around line 159) ────────
   // Written by hand: `#[derive(Clone)]` would demand `T: Clone`, yet copying
   // a ticket never copies a `T`.
   impl<T> Clone for Ticket<T> {
       fn clone(&self) -> Ticket<T> {
           Ticket::new(self.id, Arc::clone(&self.store))
       }
   }

   impl<T: Send + Sync + 'static> Ticket<T> {
       /// Analogy: look into the pot where it stands — no ladle, no second pot.
       ///
       /// Wait until the task finishes, then share its result: another pointer
       /// to the one stored value, never a copy of it.
       pub fn read(&self) -> Result<Arc<T>, TaskError> {
           let value = self.store.get(self.id)?;
           Ok(value
               .downcast::<T>()
               .expect("a ticket's type always matches its task's result"))
       }

       /// Wait until the task finishes, then hand over a copy of its result.
       /// The result stays in the store, so any copy of the ticket can `get` it.
       pub fn get(&self) -> Result<T, TaskError>
       where
           T: Clone,
       {
           let shared = self.read()?;
           Ok(T::clone(&shared))
       }
   }
   // ──────── replacement ends here ────────
   ```

Notice what did *not* need changing: the pass itself. It has kept one
copy of each value since Day 2.

Three details carry the change:

- `read` uses `downcast::<T>()` where Day 2 used `downcast_ref::<T>()`.
  `downcast_ref` lends you a `&T` that is only valid while you hold the
  box. `downcast` turns the shared box itself into an `Arc<T>`, which you
  may keep as long as you like. No `T` is copied either way.

- `get` is now "read, then copy." It writes `T::clone(&shared)` on
  purpose. `shared.clone()` would clone the *`Arc`* (one more label, no
  copy) and hand back the wrong type. `T::clone` says "copy the value
  itself." `get` keeps its `where T: Clone` bound. `read` needs none,
  because it never copies.

- Why write `Clone` by hand? `#[derive(Clone)]` adds a hidden rule: "a
  `Ticket<T>` can be cloned only if `T` can be." That rule was harmless on
  Day 2, but the next edit must copy tickets for values that cannot be
  copied. Copying a ticket copies a number and a pointer, never a `T`. So
  the hand-written version has no such rule.

Now the worker's side. In `src/worker.rs`, replace the whole
`submit_with` method (around line 62, doc comment included) with these
two methods:

```rust,ignore
    // ──────── replacement starts here (around line 62) ────────
    /// Like `submit`, but `task` reads the result behind `input` in place: it
    /// borrows the one stored value instead of getting a copy. Returns a
    /// ticket at once; the task joins the queue only once `input` has landed.
    /// If `input` failed, `task` never runs and its ticket carries the same
    /// error.
    pub fn submit_reading<A, T, F>(&self, input: &Ticket<A>, task: F) -> Ticket<T>
    where
        A: Send + Sync + 'static,
        F: FnOnce(&A) -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let id = self.store.new_id();
        let promise = Promise::new(id, Arc::clone(&self.store));
        let input_copy = input.clone();
        let job: Job = Box::new(move || {
            // Already landed by the time this job runs, so `read` won't block.
            let outcome = match input_copy.read() {
                Ok(value) => run_catching(move || task(&value)),
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

    /// Like `submit_reading`, but `task` gets its own copy of the input value.
    pub fn submit_with<A, T, F>(&self, input: &Ticket<A>, task: F) -> Ticket<T>
    where
        A: Clone + Send + Sync + 'static,
        F: FnOnce(A) -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        self.submit_reading(input, move |value: &A| task(A::clone(value)))
    }
    // ──────── replacement ends here ────────
```

`submit_reading` is Day 2's `submit_with` with three small changes:

- `A` no longer needs `Clone`.

- The job calls `read` instead of `get`.

- It calls `task(&value)`, handing the task a *shared reference* (an
  `&A`) to the stored value. A shared reference lets the task look,
  never change, and never keep the value past the call.

`value` itself is an `Arc<A>`. Writing `&value` where an `&A` is expected
works because an `Arc` hands out a reference to what it points at. Rust
inserts that step for you (*deref coercion*).

`submit_with` keeps its name and behavior and shrinks to one line:
"read in place, and give the task its own copy of what you read." All of
Day 2's `submit_with` tests now run through `submit_reading`, unchanged.

Last, finish the demo. Make two insertions in `src/main.rs`. The `⋮` and
the framing lines are already in your file:

```rust,ignore
fn one_cook_copies_or_shares() {
    ⋮
    println!("8 tasks, submit_with      -> {done} done: {}", cost(since));

    // ──────── new code starts here (around line 54) ────────
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
    // ──────── new code ends here ────────
}

// ──────── new code starts here (around line 75) ────────
/// How many of `tasks` reported the address `home`.
fn how_many_saw(tasks: &[Ticket<usize>], home: usize) -> usize {
    tasks
        .iter()
        .filter(|task| matches!(task.get(), Ok(seen) if seen == home))
        .count()
}
// ──────── new code ends here ────────
```

`how_many_saw` counts the tasks whose reported address equals `home`,
the stored value's own address. `matches!(…, Ok(seen) if seen == home)`
is the `matches!` you met on Day 2, with one addition. A pattern may end
in `if …`, and then it only matches when that condition holds too.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 5.0 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 13.4 ms, 512.0 MiB allocated
──── everything above: the first run's three lines, unchanged ────
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
```

Read the new lines one by one:

- `read #1 -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated` — the same
  64 MiB, now as a label instead of a ladle. `Ticket::read` handed back an
  `Arc<Vec<u8>>` pointing at the stored bytes.

- `8 tasks, submit_reading -> 8 done: 0.0 ms, 0.0 MiB allocated` — eight
  tasks filled zero pots. Each one borrowed the stored value through
  `submit_reading`. (A few hundred bytes for slips and tickets do get
  allocated, but they round to 0.0.)

- `saw #1's own bytes -> copies: 0 of 8, readers: 8 of 8` — here is the
  proof. None of the `submit_with` tasks looked at the stored bytes, but
  all eight `submit_reading` tasks did. `how_many_saw` compared each reported
  address with the address of `shared`.

Your timings will differ. Mine wobble between about 4 and 8 ms for `get`,
and between 10 and 16 ms for the eight copies. The memory column is the
same on every run, because the meter counts requests, not luck.

Five tests pin today's behavior. First, append the meter's own test to
the end of `src/meter.rs`, after the closing brace of `allocated`:

```rust,ignore
// ──────── new code starts here (around line 33) ────────
#[cfg(test)]
mod tests {
    use super::allocated;

    #[test]
    fn the_meter_counts_a_fresh_allocation() {
        let before = allocated();
        let block = vec![1_u8; 1 << 20];
        // Other tests allocate at the same time, so only a lower bound is certain.
        assert!(allocated() - before >= block.len());
    }
}
// ──────── new code ends here ────────
```

`1 << 20` is 2 to the power 20: 1,048,576 bytes, exactly 1 MiB. Tests
run in parallel, and all of them share the one global meter. So this test
can only promise a lower bound: "at least my 1 MiB was counted."

Then, in `src/ticket.rs`, add a test at the end of `mod tests`, right
before the file's final closing `}`:

```rust,ignore
mod tests {
    ⋮
    // ──────── new code starts here (around line 355) ────────
    #[test]
    fn read_shares_the_stored_value_and_get_copies_it() {
        let store = MemoryStore::new();
        let id = store.new_id();
        let ticket: Ticket<Vec<u8>> = Ticket::new(id, Arc::clone(&store));
        store.put(id, Ok(Arc::new(vec![7_u8; 1000])));
        let first = ticket.read().unwrap();
        let second = ticket.read().unwrap();
        assert!(Arc::ptr_eq(&first, &second), "two reads, one value");
        let copy = ticket.get().unwrap();
        assert_eq!(copy, *first);
        assert_ne!(copy.as_ptr(), first.as_ptr(), "get hands over a copy");
    }
}
    // ──────── new code ends here ────────
}
```

`Arc::ptr_eq(&first, &second)` is `true` when two `Arc`s point at the
same value: two labels, one pot.

And in `src/worker.rs`, add one helper and three tests at the end of
`mod tests`, before the file's final closing `}`:

```rust,ignore
mod tests {
    ⋮
    // ──────── new code starts here (around line 387) ────────
    /// Where a byte vector's contents sit in memory.
    fn address(bytes: &[u8]) -> usize {
        bytes.as_ptr() as usize
    }

    #[test]
    fn reading_tasks_all_see_the_one_stored_value() {
        let worker = Worker::start("worker-0");
        let stock = worker.submit(|| vec![7_u8; 1000]);
        let readers: Vec<Ticket<usize>> = (0..3)
            .map(|_| worker.submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
            .collect();
        let home = address(&stock.read().unwrap());
        for reader in readers {
            assert_eq!(reader.get().unwrap(), home);
        }
    }

    #[test]
    fn submit_with_still_hands_each_task_its_own_copy() {
        let worker = Worker::start("worker-0");
        let stock = worker.submit(|| vec![7_u8; 1000]);
        let copier = worker.submit_with(&stock, |bytes: Vec<u8>| address(&bytes));
        assert_ne!(copier.get().unwrap(), address(&stock.read().unwrap()));
    }

    #[test]
    fn a_value_that_cannot_be_copied_can_still_be_read() {
        // No `Clone` here: `get` and `submit_with` would not compile for it.
        struct Recipe {
            steps: Vec<String>,
        }
        let worker = Worker::start("worker-0");
        let recipe = worker.submit(|| Recipe {
            steps: vec!["chop".to_string(), "simmer".to_string()],
        });
        let count = worker.submit_reading(&recipe, |recipe: &Recipe| recipe.steps.len());
        assert_eq!(count.get().unwrap(), 2);
        assert_eq!(recipe.read().unwrap().steps[1], "simmer");
    }
}
    // ──────── new code ends here ────────
}
```

The third test is the reason `Clone` became hand-written. A `Recipe`
cannot be copied at all, yet tasks can still read it in place.

```bash
cargo test
```

```text
running 29 tests
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::read_shares_the_stored_value_and_get_copies_it ... ok
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test meter::tests::the_meter_counts_a_fresh_allocation ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test worker::tests::a_value_that_cannot_be_copied_can_still_be_read ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::reading_tasks_all_see_the_one_stored_value ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::submit_with_still_hands_each_task_its_own_copy ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 29 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.05s
```

All 24 earlier tests (Day 1's and Day 2's) still pass, plus five new
ones.

**Section checkpoint:** `./scripts/compare.sh v1-day-03a`.

### Section B — one pass for the whole kitchen

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/tuples-and-arrays/arrays.html">arrays (<code>[a, b]</code>)</a>
</div>

Your section A reads are free. But one cook means one slip at a time, so
a slow dish holds up everything behind it. The obvious fix is a second
cook. Watch what happens. Make two edits to `src/main.rs`. The first
stays, and the second is a temporary experiment you'll delete in a minute:

1. Add `use std::thread;` above the `use std::time::…` line.

2. Add these lines at the end of `main`. The framing lines are already
   in your file:

   ```rust,ignore
   fn main() {
       one_cook_copies_or_shares();

       // ──────── new code starts here (around line 26) ────────
       let cook_1 = Worker::start("worker-1");
       let cook_2 = Worker::start("worker-2");
       let quick = cook_1.submit(|| 1_u64);
       println!("worker-1's quick dish     -> ticket {}", quick.id());
       let slow = cook_2.submit(|| {
           thread::sleep(Duration::from_secs(1));
           2_u64
       });
       println!("worker-2's slow dish      -> ticket {}", slow.id());
       let clock = Instant::now();
       let (ready, _) = wait(vec![quick, slow.clone()], 2, Duration::from_secs(5));
       let ms = clock.elapsed().as_millis();
       println!(
           "wait for both, max 5 s    -> {} ready after {ms} ms",
           ready.len()
       );
       let (ready, _) = wait(vec![slow], 1, Duration::ZERO);
       println!("worker-2's dish, alone    -> ready: {}", ready.len() == 1);
       // ──────── new code ends here ────────
   }
   ```

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 6.3 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 14.4 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
──── everything above: section A's output, unchanged ────
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #1
wait for both, max 5 s    -> 2 ready after 0 ms
worker-2's dish, alone    -> ready: false
```

Read the last four lines:

- `worker-1's quick dish -> ticket #1` and `worker-2's slow dish -> ticket
  #1` — two different dishes get the same number. Each `Worker::start`
  made its cook a brand-new pass, and each pass counts from 1.

- `wait for both, max 5 s -> 2 ready after 0 ms` — this is wrong. The
  slow dish needs a full second. `wait` looks on the *first* ticket's pass
  only, and on worker-1's pass, number #1 is indeed ready — twice. Day 2's
  `wait` promised only that all tickets come from the same worker, and
  this demo broke that promise.

- `worker-2's dish, alone -> ready: false` — asked on its own pass, the
  slow dish is honestly not ready. The same question got two answers.

The fix is to stop giving every cook its own pass. A kitchen has one
pass, and every cook puts dishes on it. Delete those temporary lines
from `main` (keep `use std::thread;`), then make two edits to
`src/worker.rs`:

1. Split `Worker::start` in two. Replace its doc comment and `pub fn
   start(name: &str) -> Worker {` line (around line 25) with these lines:

   ```rust,ignore
       // ──────── replacement starts here (around line 25) ────────
       /// Start a named worker thread, waiting for jobs, with a pass of its own.
       pub fn start(name: &str) -> Worker {
           Worker::start_in(name, &MemoryStore::new())
       }

       /// Analogy: hire a cook into a kitchen whose pass is already there — every
       /// cook in the kitchen puts dishes on the same pass.
       ///
       /// Start a named worker thread whose results land in `store`, which other
       /// workers may share. One store means one numbering and one place to look.
       pub fn start_in(name: &str, store: &Arc<MemoryStore>) -> Worker {
       // ──────── replacement ends here ────────
   ```

   The rest of the old `start` body is now `start_in`'s body.

2. At the end of that body, the cook stops making a pass of its own.
   Change the `store: MemoryStore::new(),` line (around line 49) to:

   ```rust,ignore
               store: Arc::clone(store),
   ```

`start` keeps its old behavior (one cook, a pass of its own) by asking
`start_in` for exactly that. So all of Day 1 and Day 2's code is
untouched. `start_in` takes the pass as `&Arc<MemoryStore>`. It borrows
your handle and keeps an `Arc::clone` of it: one more handle on the same
shared pass.

One doc comment is now out of date. In `src/ticket.rs`, the doc of
`pub fn wait` (around line 196) ends "All tickets must come from the same
worker." Change `worker` to `store` (in code, the pass is a store). It was
always the pass that mattered.

Now the demo. Make three edits to `src/main.rs`:

1. Import the pass type: change `use ticket::{Ticket, wait};` to
   `use ticket::{MemoryStore, Ticket, wait};`.

2. Call the new scene. Add `two_cooks_one_pass();` as the last line of
   `main`.

3. After the closing brace of `how_many_saw` (around line 85), add the
   scene and two helpers. The last one is Day 2's `round_to_100`, back
   again:

   ```rust,ignore
   // ──────── new code starts here (around line 85) ────────
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
   // ──────── new code ends here ────────
   ```

`let cooks = [ … ];` is an *array*: a fixed-size list, here of exactly two
workers, reached as `cooks[0]` and `cooks[1]`. `cook_name` reads the
current thread's name. Inside a task, that is the name of the cook
running it.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 4.2 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 10.7 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
──── everything above: section A's output, unchanged ────
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #2
wait for both, max 5 s    -> 2 ready after about 1000 ms
worker-1 makes the stock  -> ticket #3
worker-2 reads #3         -> #3's own bytes: true, 0.0 ms, 0.0 MiB allocated
```

Read the new lines one by one:

- `worker-1's quick dish -> ticket #1`, `worker-2's slow dish -> ticket
  #2` — one pass gives one numbering. Both cooks' slips drew their
  numbers from the same `new_id` counter.

- `wait for both, max 5 s -> 2 ready after about 1000 ms` — now `wait` is
  honest. It finds both numbers on the one pass, and returns when the slow
  dish lands.

- `worker-1 makes the stock -> ticket #3` — one cook makes the 64 MiB.

- `worker-2 reads #3 -> #3's own bytes: true, … 0.0 MiB allocated` — the
  *other* cook reads it in place. `cook_name` proved which cook ran the
  task, and the address proved it looked at #3's own bytes. With one pass
  and one pantry behind it, every cook reads every dish.

Three tests pin this behavior. In `src/worker.rs`'s `mod tests`:

1. Change `use crate::ticket::{MemoryStore, Ticket};` (around line 202) to
   `use crate::ticket::{MemoryStore, Ticket, wait};`.

2. Add three tests at the end of `mod tests`, before the file's final
   closing `}`:

   ```rust,ignore
   mod tests {
       ⋮
       // ──────── new code starts here (around line 437) ────────
       #[test]
       fn workers_on_one_store_share_one_numbering() {
           let store = MemoryStore::new();
           let one = Worker::start_in("worker-1", &store);
           let two = Worker::start_in("worker-2", &store);
           let ids = [
               one.submit(|| 1_u64).id().to_string(),
               two.submit(|| 2_u64).id().to_string(),
               one.submit(|| 3_u64).id().to_string(),
           ];
           assert_eq!(ids, ["#1", "#2", "#3"]);
       }

       #[test]
       fn wait_sees_results_from_every_worker_on_the_store() {
           let store = MemoryStore::new();
           let one = Worker::start_in("worker-1", &store);
           let two = Worker::start_in("worker-2", &store);
           let (open_gate, gate) = mpsc::channel::<()>();
           let quick = one.submit(|| 1_u64);
           let held = two.submit(move || {
               gate.recv().unwrap();
               2_u64
           });
           // worker-1's dish has landed; worker-2's is still behind the gate.
           quick.get().unwrap();
           let (ready_now, _) = wait(vec![quick.clone(), held.clone()], 2, Duration::ZERO);
           assert_eq!(ready_now.len(), 1, "only worker-1's dish is ready");
           open_gate.send(()).unwrap();
           // A generous timeout: the test passes only if worker-2's dish is seen.
           let (ready_now, _) = wait(vec![quick, held], 2, Duration::from_secs(60));
           assert_eq!(ready_now.len(), 2);
       }

       #[test]
       fn one_worker_reads_another_workers_result_in_place() {
           let store = MemoryStore::new();
           let one = Worker::start_in("worker-1", &store);
           let two = Worker::start_in("worker-2", &store);
           let stock = one.submit(|| vec![7_u8; 1000]);
           let reader = two.submit_reading(&stock, |bytes: &Vec<u8>| {
               let cook = thread::current().name().unwrap_or("?").to_string();
               (cook, address(bytes))
           });
           let home = address(&stock.read().unwrap());
           assert_eq!(reader.get().unwrap(), ("worker-2".to_string(), home));
       }
   }
       // ──────── new code ends here ────────
   }
   ```

All three use workers started on one shared pass.
`wait_sees_results_from_every_worker_on_the_store` uses a gate, as on
Day 2. Worker-2's dish provably cannot be ready, so exactly one of the two
tickets may be. The bug you watched a moment ago would report two.

```bash
cargo test
```

```text
running 32 tests
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::read_shares_the_stored_value_and_get_copies_it ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test meter::tests::the_meter_counts_a_fresh_allocation ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test worker::tests::a_value_that_cannot_be_copied_can_still_be_read ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::reading_tasks_all_see_the_one_stored_value ... ok
test worker::tests::one_worker_reads_another_workers_result_in_place ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::submit_with_still_hands_each_task_its_own_copy ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::workers_on_one_store_share_one_numbering ... ok
test worker::tests::wait_sees_results_from_every_worker_on_the_store ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok

test result: ok. 32 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

All 29 earlier tests pass, plus the three new ones.

**Section checkpoint:** `./scripts/compare.sh v1-day-03b`.

### Section C — the waiter stocks the pantry

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/references/exclusive.html">exclusive references (<code>&amp;mut T</code>)</a>
</div>

Every value in the pantry so far was made by a cook. But often the
waiter already holds the big value: a file it read, a table it built, a
delivery that just arrived. How does that value reach the tasks? Try the
two ways you have. Add these lines at the end of `main`, temporarily. The
framing lines are already in your file:

```rust,ignore
fn main() {
    one_cook_copies_or_shares();
    two_cooks_one_pass();

    // ──────── new code starts here (around line 27) ────────
    let cook = Worker::start("worker-3");
    let delivery = make_stock();
    let since = start_measuring();
    let tasks: Vec<Ticket<usize>> = (0..8)
        .map(|_| {
            let mine = delivery.clone();
            cook.submit(move || address(&mine))
        })
        .collect();
    println!("8 tasks, a clone each     -> {}", cost(since));
    wait(tasks, 8, Duration::from_secs(5));

    let _busy = cook.submit(|| thread::sleep(Duration::from_millis(300)));
    let stocked = cook.submit(move || delivery);
    let (ready, _) = wait(vec![stocked.clone()], 1, Duration::ZERO);
    println!(
        "delivery through the cook -> ticket {}, ready: {}",
        stocked.id(),
        ready.len() == 1
    );
    let clock = Instant::now();
    let _ = stocked.read();
    println!(
        "ready after about         -> {} ms",
        round_to_100(clock.elapsed())
    );
    // ──────── new code ends here ────────
}
```

Two small marks sit in there. `_busy` starts with an underscore to tell
Rust "I keep this ticket on purpose, without reading it." And `let _ =
stocked.read();` waits for the value and throws the result away, because
only the waiting matters here.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 4.2 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 10.8 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #2
wait for both, max 5 s    -> 2 ready after about 1000 ms
worker-1 makes the stock  -> ticket #3
worker-2 reads #3         -> #3's own bytes: true, 0.0 ms, 0.0 MiB allocated
──── everything above: sections A and B's output, unchanged ────
8 tasks, a clone each     -> 9.9 ms, 512.0 MiB allocated
delivery through the cook -> ticket #10, ready: false
ready after about         -> 300 ms
```

Read it line by line:

- `8 tasks, a clone each -> … 512.0 MiB allocated` — each closure needs
  its own value to `move` in, so each got a `delivery.clone()`. That is
  section A's 512 MiB again, this time made on the waiter's side.

- `delivery through the cook -> ticket #10, ready: false` — dressing the
  delivery up as an order slip avoids the copies, but now it waits in
  line. The cook is busy for 300 ms, and the "order" (hand the delivery
  back) sits on the rail behind that slip. (It is number #10 because
  `worker-3` has a pass of its own. The eight tasks took #1 to #8, and the
  300 ms slip took #9.)

- `ready after about -> 300 ms` — `stocked.read()` blocked for the cook's
  300 ms slip. A value the waiter held the whole time only became
  readable once the cook got around to it.

Ray's docs list both moves as anti-patterns. The fix is a way in that
needs no cook at all. The waiter carries the value straight into the
pantry and gets a ticket on the spot. Delete the temporary lines, then
make one edit to `src/ticket.rs`. After the closing brace of `pub fn
wait` and before `#[cfg(test)]` (around line 222), add:

```rust,ignore
// ──────── new code starts here (around line 222) ────────
/// Analogy: the waiter stocks the pantry directly — no order slip, no cook —
/// and gets a ticket for it at once.
///
/// Like Ray's `ray.put`: store `value` under a fresh id and return its ticket,
/// already ready. The value moves in without being copied, and from now on it
/// is shared, never changed.
pub fn put<T: Send + Sync + 'static>(store: &Arc<MemoryStore>, value: T) -> Ticket<T> {
    let id = store.new_id();
    store.put(id, Ok(Arc::new(value)));
    Ticket::new(id, Arc::clone(store))
}
// ──────── new code ends here ────────
```

Two notes:

- There are now two `put`s, and they do different jobs.
  `MemoryStore::put(id, outcome)` is Day 2's. It fills a spot whose number
  someone already holds, and [the cook's stub](story.md#stub) uses it. The
  new free function `put(&store, value)` is Ray's `ray.put`. It picks a
  fresh number itself, fills the spot, and hands back the ticket. It is
  built from the first.

- `value: T` is taken *by value*: moved in, not copied. For a `Vec`,
  moving copies only its small header (where the bytes are, how many).
  The 64 MiB of bytes never move. `Arc::new` then wraps that header in a
  shared box. The waiter's original bytes become the pantry's bytes.

Now the scene. Make three edits to `src/main.rs`:

1. Change `use ticket::{MemoryStore, Ticket, wait};` to
   `use ticket::{MemoryStore, Ticket, put, wait};`.

2. Add `the_waiter_stocks_the_pantry();` as the last line of `main`.

3. After the closing brace of `two_cooks_one_pass` (around line 121),
   add:

   ```rust,ignore
   // ──────── new code starts here (around line 121) ────────
   /// The waiter brings a big value no cook made and puts it in the pantry once.
   fn the_waiter_stocks_the_pantry() {
       let pass = MemoryStore::new();
       let cooks = [
           Worker::start_in("worker-1", &pass),
           Worker::start_in("worker-2", &pass),
       ];
       let busy: Vec<Ticket<()>> = cooks
           .iter()
           .map(|cook| cook.submit(|| thread::sleep(Duration::from_millis(300))))
           .collect();
       println!(
           "both cooks busy, 300 ms   -> tickets {} and {}",
           busy[0].id(),
           busy[1].id()
       );

       let delivery = make_stock();
       let home = address(&delivery);
       let since = start_measuring();
       let stock = put(&pass, delivery);
       let id = stock.id();
       let (ready, _) = wait(vec![stock.clone()], 1, Duration::ZERO);
       let ready = ready.len() == 1;
       println!(
           "put the delivery          -> ticket {id}, ready: {ready}, {}",
           cost(since)
       );
       let (done, _) = wait(busy, 2, Duration::ZERO);
       println!("cooks finished yet?       -> {} of 2", done.len());

       let since = start_measuring();
       let readers: Vec<Ticket<usize>> = (0..8)
           .map(|n| cooks[n % 2].submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
           .collect();
       let (readers, _) = wait(readers, 8, Duration::from_secs(5));
       let done = readers.len();
       println!("8 tasks on 2 cooks        -> {done} done: {}", cost(since));
       let seen = how_many_saw(&readers, home);
       println!("saw the delivery's bytes  -> {seen} of 8");
   }
   // ──────── new code ends here ────────
   ```

`cooks[n % 2]` hands the eight readers to the two cooks in turn (`%` is
the remainder: 0, 1, 0, 1, …). Choosing a cook by hand is clumsy, and on
Day 4 a scheduler takes that job over.

`cargo run`:

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 3.8 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 10.8 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #2
wait for both, max 5 s    -> 2 ready after about 1000 ms
worker-1 makes the stock  -> ticket #3
worker-2 reads #3         -> #3's own bytes: true, 0.0 ms, 0.0 MiB allocated
──── everything above: sections A and B's output, unchanged ────
both cooks busy, 300 ms   -> tickets #1 and #2
put the delivery          -> ticket #3, ready: true, 0.0 ms, 0.0 MiB allocated
cooks finished yet?       -> 0 of 2
8 tasks on 2 cooks        -> 8 done: 303.5 ms, 0.0 MiB allocated
saw the delivery's bytes  -> 8 of 8
```

Read the new lines one by one:

- `both cooks busy, 300 ms -> tickets #1 and #2` — both cooks get a slow
  slip, so neither can help for 300 ms.

- `put the delivery -> ticket #3, ready: true, 0.0 ms, 0.0 MiB allocated`
  — the waiter stocks the pantry. `put` drew number #3 from the same
  counter the cooks use, stored the value, and returned a ticket that is
  ready at once. There was no cook and no copy.

- `cooks finished yet? -> 0 of 2` — this proves that `put` did not wait
  for a cook, because both cooks are still busy.

- `8 tasks on 2 cooks -> 8 done: 303.5 ms, 0.0 MiB allocated` — the
  readers wait their turn behind the slow slips, then read the delivery in
  place. The wait is about 300 ms, because both cooks work at the same
  time.

- `saw the delivery's bytes -> 8 of 8` — every reader looked at the very
  bytes the waiter made before calling `put`. The value was put in once,
  read by everyone, and never copied.

**Why sealed?** Everything in the pantry is read-only, for every reader.
See what happens if a reader tries to change the pot. Temporarily change
the readers' closure in `the_waiter_stocks_the_pantry` to:

```rust,ignore
        .map(|n| {
            cooks[n % 2].submit_reading(&stock, |bytes: &Vec<u8>| {
                bytes[0] = 0;
                address(bytes)
            })
        })
```

`cargo build` refuses:

```text
error[E0596]: cannot borrow `*bytes` as mutable, as it is behind a `&` reference
   --> src/main.rs:157:17
    |
157 |                 bytes[0] = 0;
    |                 ^^^^^ `bytes` is a `&` reference, so it cannot be borrowed as mutable
    |
help: consider changing this to be a mutable reference
    |
156 |             cooks[n % 2].submit_reading(&stock, |bytes: &mut Vec<u8>| {
    |                                                          +++

For more information about this error, try `rustc --explain E0596`.
error: could not compile `nano-ray-rs` (bin "nano-ray-rs") due to 1 previous error
```

The compiler's hint suggests `&mut Vec<u8>`. Follow it, and the compiler
refuses again. This time it points at `submit_reading` itself:

```text
error[E0631]: type mismatch in closure arguments
   --> src/main.rs:156:26
    |
156 |             cooks[n % 2].submit_reading(&stock, |bytes: &mut Vec<u8>| {
    |                          ^^^^^^^^^^^^^^         --------------------- found signature defined here
    |                          |
    |                          expected due to this
    |
    = note: expected closure signature `for<'a> fn(&'a Vec<_>) -> _`
               found closure signature `fn(&mut Vec<_>) -> _`
note: required by a bound in `Worker::submit_reading`
   --> src/worker.rs:79:12
    |
 76 |     pub fn submit_reading<A, T, F>(&self, input: &Ticket<A>, task: F) -> Ticket<T>
    |            -------------- required by a bound in this associated function
...
 79 |         F: FnOnce(&A) -> T + Send + 'static,
    |            ^^^^^^^^^^^^^^^ required by this bound in `Worker::submit_reading`

For more information about this error, try `rustc --explain E0631`.
error: could not compile `nano-ray-rs` (bin "nano-ray-rs") due to 1 previous error
```

`&T` is a *shared* reference: many may exist at once, and none may
change the value. `&mut T` is an *exclusive* reference: it may change
the value, but only while no one else can see it. Everyone sees a pantry
pot, so exclusive access is impossible by construction. The compiler
enforces `submit_reading`'s promise, `FnOnce(&A)`, on every task. Put the
closure back.

That refusal is the whole safety story of the pantry. A stored value
never changes, and that gives you three things:

- **Readers need no lock.** The pass's `Mutex` is held only for the
  moment it takes to find a label. The reading itself happens with no
  lock at all, on as many threads as you like.

- **Nobody can read a half-changed value**, and nobody's answer changes
  under them.

- **A copy can never go stale.** That will matter on Day 9, when other
  machines keep copies of pantry values.

If a task wants a changed version, it makes a new value and returns it:
a new dish, under a new number. The old one stays exactly as it was.

Five tests pin `put`. In `src/ticket.rs`'s `mod tests`:

1. Change `use super::{MemoryStore, Outcome, Ticket, wait};` (around line
   236) to `use super::{MemoryStore, Outcome, Ticket, put, wait};`.

2. Add two tests at the end of `mod tests`, before the file's final
   closing `}`:

   ```rust,ignore
   mod tests {
       ⋮
       // ──────── new code starts here (around line 381) ────────
       #[test]
       fn put_is_ready_at_once_and_needs_no_worker() {
           let store = MemoryStore::new();
           let ticket = put(&store, 42_u64);
           let (ready_now, _) = wait(vec![ticket.clone()], 1, Duration::ZERO);
           assert_eq!(ready_now.len(), 1);
           assert_eq!(ticket.get().unwrap(), 42);
       }

       #[test]
       fn put_moves_the_value_in_without_copying_it() {
           let store = MemoryStore::new();
           let bytes = vec![7_u8; 1000];
           let home = bytes.as_ptr();
           let ticket = put(&store, bytes);
           assert_eq!(ticket.read().unwrap().as_ptr(), home);
       }
   }
       // ──────── new code ends here ────────
   }
   ```

And in `src/worker.rs`'s `mod tests`:

1. Change `use crate::ticket::{MemoryStore, Ticket, wait};` (around line
   202) to `use crate::ticket::{MemoryStore, Ticket, put, wait};`.

2. Add three tests at the end of `mod tests`, before the file's final
   closing `}`:

   ```rust,ignore
   mod tests {
       ⋮
       // ──────── new code starts here (around line 485) ────────
       #[test]
       fn put_values_and_task_results_share_one_numbering() {
           let store = MemoryStore::new();
           let worker = Worker::start_in("worker-0", &store);
           let ids = [
               worker.submit(|| 1_u64).id().to_string(),
               put(&store, 2_u64).id().to_string(),
               worker.submit(|| 3_u64).id().to_string(),
           ];
           assert_eq!(ids, ["#1", "#2", "#3"]);
       }

       #[test]
       fn a_put_value_feeds_tasks_like_any_result() {
           let store = MemoryStore::new();
           let worker = Worker::start_in("worker-0", &store);
           let stock = put(&store, vec![7_u8; 1000]);
           let home = address(&stock.read().unwrap());
           let reader = worker.submit_reading(&stock, |bytes: &Vec<u8>| address(bytes));
           let total = worker.submit_with(&stock, |bytes: Vec<u8>| bytes.len());
           assert_eq!(reader.get().unwrap(), home);
           assert_eq!(total.get().unwrap(), 1000);
       }

       #[test]
       fn put_does_not_wait_for_a_busy_worker() {
           let store = MemoryStore::new();
           let worker = Worker::start_in("worker-0", &store);
           let (open_gate, gate) = mpsc::channel::<()>();
           let busy = worker.submit(move || gate.recv().unwrap());
           // The worker is stuck behind the gate, yet the put value is ready.
           let stock = put(&store, 5_u64);
           let (ready_now, _) = wait(vec![stock], 1, Duration::ZERO);
           assert_eq!(ready_now.len(), 1);
           open_gate.send(()).unwrap();
           busy.get().unwrap();
       }
   }
       // ──────── new code ends here ────────
   }
   ```

None of them measure time. `put_moves_the_value_in_without_copying_it`
compares addresses, the same proof the demo prints.
`put_does_not_wait_for_a_busy_worker` keeps the worker provably busy
behind a gate while it checks that the put value is ready.

```text
running 37 tests
test meter::tests::the_meter_counts_a_fresh_allocation ... ok
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::put_moves_the_value_in_without_copying_it ... ok
test ticket::tests::read_shares_the_stored_value_and_get_copies_it ... ok
test ticket::tests::put_is_ready_at_once_and_needs_no_worker ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::a_value_that_cannot_be_copied_can_still_be_read ... ok
test worker::tests::a_put_value_feeds_tasks_like_any_result ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::put_values_and_task_results_share_one_numbering ... ok
test worker::tests::put_does_not_wait_for_a_busy_worker ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::reading_tasks_all_see_the_one_stored_value ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::one_worker_reads_another_workers_result_in_place ... ok
test worker::tests::submit_with_still_hands_each_task_its_own_copy ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::wait_sees_results_from_every_worker_on_the_store ... ok
test worker::tests::workers_on_one_store_share_one_numbering ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok

test result: ok. 37 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

**Section checkpoint:** `./scripts/compare.sh v1-day-03c` — which is also
the finished day.

## 7. Run it

```bash
cargo run
```

## 8. Expected result

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 3.8 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 10.8 ms, 512.0 MiB allocated
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
8 tasks on 2 cooks        -> 8 done: 303.5 ms, 0.0 MiB allocated
saw the delivery's bytes  -> 8 of 8
```

The whole evening in the kitchen, one stretch at a time:

- The first six lines — one cook, one pot of stock. Ladling it out (`get`,
  `submit_with`) fills a fresh 64 MiB pot every time. Reading it in place
  (`read`, `submit_reading`) costs nothing, and every reader sees the
  stock's own bytes.

- The next five — two cooks on one pass. They share one numbering and an
  honest `wait`, and one cook reads the other's dish in place.
  (`Worker::start_in`.)

- The last five — the waiter stocks the pantry with `put` while both
  cooks are busy. The value is ready at once and nothing is copied. Eight
  readers on two cooks see the waiter's own bytes.

Only the millisecond figures vary between runs, and only the
`about 1000 ms` line rounds them. Everything else is the same on every
run: every ticket number, every `MiB allocated`, every count. (One honest
caveat, as on Day 2: two demo lines rely on timing. `cooks finished
yet? -> 0 of 2` assumes `put` takes less than the cooks' 300 ms, and the
slow dish's 1 s keeps `wait` visibly waiting. The tests rely on gates
instead.)

## 9. Test it

Thirty-seven tests now pin the engine: Day 2's 24, plus 13 new ones
today. Section A's 5 pin the meter, shared reads and the copy they
replace. Section B's 3 pin several workers on one store. Section C's 5
pin `put`:

```bash
cargo test
```

```text
running 37 tests
test meter::tests::the_meter_counts_a_fresh_allocation ... ok
test ticket::tests::wait_on_no_tickets_returns_at_once ... ok
test worker::tests::a_job_thrown_away_unrun_reports_worker_stopped ... ok
test ticket::tests::when_ready_runs_at_once_if_the_result_is_already_there ... ok
test ticket::tests::ids_are_unique_and_count_up_from_one ... ok
test ticket::tests::every_copy_of_a_ticket_reads_the_same_result ... ok
test ticket::tests::put_moves_the_value_in_without_copying_it ... ok
test ticket::tests::read_shares_the_stored_value_and_get_copies_it ... ok
test ticket::tests::put_is_ready_at_once_and_needs_no_worker ... ok
test ticket::tests::wait_returns_ready_tickets_in_order_and_at_most_num_ready ... ok
test ticket::tests::when_ready_holds_a_job_until_the_result_lands ... ok
test ticket::tests::get_blocks_until_another_thread_puts_the_result ... ok
test ticket::tests::wait_wakes_as_soon_as_enough_results_land ... ok
test worker::tests::a_held_task_joins_the_queue_when_its_input_lands ... ok
test worker::tests::a_failed_input_skips_the_task_and_passes_the_error_on ... ok
test worker::tests::a_burnt_dish_reads_the_same_through_every_copy ... ok
test ticket::tests::a_failed_task_counts_as_ready ... ok
test worker::tests::a_panicking_task_becomes_an_error_on_its_ticket ... ok
test worker::tests::a_value_that_cannot_be_copied_can_still_be_read ... ok
test worker::tests::a_put_value_feeds_tasks_like_any_result ... ok
test worker::tests::a_panic_in_the_second_task_stays_on_its_own_ticket ... ok
test worker::tests::formatted_panic_messages_survive_too ... ok
test worker::tests::jobs_run_on_the_worker_thread_in_order ... ok
test worker::tests::get_returns_the_task_result ... ok
test worker::tests::put_values_and_task_results_share_one_numbering ... ok
test worker::tests::put_does_not_wait_for_a_busy_worker ... ok
test worker::tests::one_ticket_can_feed_several_tasks_and_chains ... ok
test worker::tests::reading_tasks_all_see_the_one_stored_value ... ok
test worker::tests::submit_returns_before_the_task_finishes ... ok
test worker::tests::one_worker_reads_another_workers_result_in_place ... ok
test worker::tests::submit_with_still_hands_each_task_its_own_copy ... ok
test worker::tests::submit_with_returns_before_its_input_is_ready ... ok
test worker::tests::the_worker_survives_a_panicking_task ... ok
test worker::tests::wait_sees_results_from_every_worker_on_the_store ... ok
test worker::tests::workers_on_one_store_share_one_numbering ... ok
test worker::tests::dropping_the_worker_waits_for_queued_jobs ... ok
test ticket::tests::wait_gives_up_when_the_timeout_runs_out ... ok

test result: ok. 37 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

(All 37 are unit tests. The program still takes no command-line input,
so there is nothing for a command-line test to drive.)

## 10. Break it deliberately

Give every cook a private pass again. In `Worker::start_in` in
`src/worker.rs`, change one line back to what it was on Day 2:

```diff
-            store: Arc::clone(store),
+            store: MemoryStore::new(),
```

It compiles, with one warning: `unused variable: store`. The shared pass
handed to `start_in` is now ignored. `cargo run` finishes, and nothing
crashes. But read the second half closely (the whole output, warning
excluded):

```text
submit make_stock         -> ticket #1, ready: true
get #1                    -> 64 MiB of bytes: 4.4 ms, 64.0 MiB allocated
8 tasks, submit_with      -> 8 done: 11.6 ms, 512.0 MiB allocated
read #1                   -> 64 MiB of bytes: 0.0 ms, 0.0 MiB allocated
8 tasks, submit_reading   -> 8 done: 0.0 ms, 0.0 MiB allocated
saw #1's own bytes        -> copies: 0 of 8, readers: 8 of 8
worker-1's quick dish     -> ticket #1
worker-2's slow dish      -> ticket #1
wait for both, max 5 s    -> 2 ready after about 0 ms
worker-1 makes the stock  -> ticket #2
worker-2 reads #2         -> #2's own bytes: true, 1009.5 ms, 0.0 MiB allocated
both cooks busy, 300 ms   -> tickets #1 and #1
put the delivery          -> ticket #1, ready: true, 0.0 ms, 0.0 MiB allocated
cooks finished yet?       -> 0 of 2
8 tasks on 2 cooks        -> 8 done: 305.0 ms, 0.0 MiB allocated
saw the delivery's bytes  -> 8 of 8
```

Read it as a list of lies:

- `ticket #1` twice, then `2 ready after about 0 ms` — section B's bug is
  back. Two dishes share a number, and `wait` believes the slow one is
  done.

- `worker-2 reads #2 -> …, 1009.5 ms` — here is the price of that
  lie. The program moved on while worker-2 was still cooking its slow
  dish, so the read had to queue behind it for a full second.

- `tickets #1 and #1`, then `put the delivery -> ticket #1` — the waiter's
  delivery got number #1 too. Three different values share one number.

- The last two lines look right, and that is the scariest part. `wait`
  checked the readers' numbers on worker-1's pass, where *other* dishes
  with the same numbers happened to be ready. They are correct only by
  coincidence.

`cargo test` is blunter (lines trimmed to the interesting ones):

```text
running 37 tests
test worker::tests::put_values_and_task_results_share_one_numbering ... FAILED
test worker::tests::workers_on_one_store_share_one_numbering ... FAILED
test worker::tests::wait_sees_results_from_every_worker_on_the_store ... FAILED

failures:

---- worker::tests::put_values_and_task_results_share_one_numbering stdout ----

thread 'worker::tests::put_values_and_task_results_share_one_numbering' (320478291) panicked at src/worker.rs:494:9:
assertion `left == right` failed
  left: ["#1", "#1", "#2"]
 right: ["#1", "#2", "#3"]

---- worker::tests::workers_on_one_store_share_one_numbering stdout ----

thread 'worker::tests::workers_on_one_store_share_one_numbering' (320478306) panicked at src/worker.rs:447:9:
assertion `left == right` failed
  left: ["#1", "#1", "#2"]
 right: ["#1", "#2", "#3"]

---- worker::tests::wait_sees_results_from_every_worker_on_the_store stdout ----

thread 'worker::tests::wait_sees_results_from_every_worker_on_the_store' (320478305) panicked at src/worker.rs:464:9:
assertion `left == right` failed: only worker-1's dish is ready
  left: 2
 right: 1

thread 'worker-2' (320478309) panicked at src/worker.rs:458:25:
called `Result::unwrap()` on an `Err` value: RecvError

test result: FAILED. 34 passed; 3 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

- `workers_on_one_store_share_one_numbering` and
  `put_values_and_task_results_share_one_numbering` catch the duplicate
  numbers. They see `["#1", "#1", "#2"]` where `["#1", "#2", "#3"]`
  belongs.

- `wait_sees_results_from_every_worker_on_the_store` catches the lying
  glance: two tickets ready where only one can be. (The extra `worker-2`
  panic comes from that test's gated task. The failed test dropped the
  gate's other end, so the task's `recv` found no sender.)

The lesson: **a ticket number means something only inside one store.**
Ray faces the same rule at the scale of a cluster. Its object ids are
unique everywhere, so a ref can be handed to any worker on any machine
and still name exactly one object.

Change the line back and confirm `cargo test` is green again.

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
./scripts/compare.sh v1-day-03
```

**Inspecting instead of typing?** Jump to today's exact finished state,
poke at it, and come back:

```bash
git switch --detach v1-day-03
cargo test
git switch -          # back to where you were
```

**Either way**, today's complete change set — docs included — reads as one
diff:

```bash
git diff v1-day-02 v1-day-03
```

## 12. Recap

**What changed today:** a meter now counts every byte of fresh memory,
and it showed what Day 2's collection really cost: a full copy per
reader. `Ticket::read` hands out the stored value itself, as an `Arc`:
one more label on one pot. `submit_reading` lends that value to a task as
a read-only `&A`. `get` and `submit_with` became "read, then copy."
Workers started with `Worker::start_in` share one pass, so they share one
numbering, one place to `wait`, and each other's results. `put` stores a
value the program already holds: ready at once, moved in, never copied.
And the compiler keeps every stored value sealed. Thirteen new tests (37
in all) pin it across three section checkpoints (`v1-day-03a`,
`v1-day-03b`, `v1-day-03c`), still with no dependencies.

**Deliberately naive:** nothing is ever removed. In a long-running
program the pass only grows, and so does the pantry behind it, and nobody
yet knows when a value is safe to delete. The pantry holds Rust values,
not bytes, so only threads of one program can share it. You pick a cook
for every task by hand. And each cook still has a rail of its own, so a
slip waits behind its own cook's queue even when the other cook is idle.

**Tomorrow's limitation, resolved on Day 4:** two cooks, two rails, and
you choosing the cook for every slip. On Day 4 the kitchen gets a pool of
cooks behind **one** queue, and a scheduler that hands each slip to
whichever cook is free. You will measure a slow job dropping to a bit
more than a quarter of its time. Today's pantry is what lets all those
cooks share their inputs without copying them. Tonight the kitchen closes with one
pass, one pantry, and a waiter who can stock it — see
[the restaurant](story.md).
