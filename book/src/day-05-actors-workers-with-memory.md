# Day 5: Actors — workers with memory

> **Day 5 of 17** · builds on `v1-day-04` · ≈ 2 h

| Section | Builds | Checkpoint |
|---------|--------|------------|
| A | The bartender keeps the tab: an actor, its state, and its mailbox | `v1-day-05a` |
| B | Two bartenders, one pass: actors side by side, and a call that panics | `v1-day-05b` |
| C | The crew pours, the bartender writes: calls that wait for a ticket | `v1-day-05c` = `v1-day-05` |

**Pick your mode before starting** (explained fully in
[Setup](setup.md#4-choose-how-you-follow-along)):

```bash
# Build mode — start (or restart) your branch from yesterday's checkpoint.
# Already on my-course from Day 4? Keep going — no command needed.
git switch -c my-course v1-day-04

# Inspect mode — read along with the finished code instead:
git switch --detach v1-day-05a         # then 05b, 05c as you pass each section
```

<div class="callout">

**Today's typing budget** — so nothing sneaks up on you:

| Section | The learning | Wiring & tests (copy-friendly) |
|---------|--------------|--------------------------------|
| A | `Actor`: `start`, `call`, `send`, `Drop` · ~55 lines of code (plus comments) in a new `src/actor.rs` | the same four `pub(crate)` marks as on Day 4, the `Tabs` demo state, a print helper, two demo functions, 5 tests |
| B | nothing new in `Actor` — you put it to work | one small method on `Tabs`, one demo function, 5 tests |
| C | `Actor::call_reading` · ~20 lines | one demo function, 3 tests |

Section A is the longest sitting of the day. Section B adds no engine code
at all: it runs the actor through its paces. Section C is short. Three
sections are three natural sittings, and every checkpoint is a save point
— nothing says a "day" must fit in one. Still **no dependencies**:
everything comes from Rust's standard library.

</div>

## 1. What you will have running

Since Day 4, a pool of workers runs any job, and whichever worker is free
takes the next one. That is what makes the pool fast. It also means no
worker remembers anything from one job to the next. By the end of today,
`cargo run` does four new things:

- **shows what goes wrong when jobs share a running total.** Three jobs
  each add a price to one total, kept behind a lock, and a fourth job
  reads the total out. Run on the pool, the quickest job runs first, so
  the total read out is $0.

- **gives the running total a home of its own: an *actor*.** One
  dedicated worker owns the total. Jobs for it wait in its own queue and
  run one at a time, in the order they were sent. The total comes out
  right, but twice as slow.

- **runs two actors side by side, and crashes one of their jobs on
  purpose.** The actors never wait for each other, the pool can read
  what they hand back, and a crash comes back as an error while the actor
  keeps working.

- **combines the two.** The pool does the slow part of each job in
  parallel, and the actor records the results, in order. You get the right total, as
  fast as the pool.

You are building Ray's actor in miniature: **a worker that remembers, fed through a
mailbox that keeps the order.**

<div class="dl-concepts"><span>Ray concepts used here:</span>
<a href="https://docs.ray.io/en/latest/ray-core/actors.html">actors ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/actors/task-orders.html">actor task execution order ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/patterns/global-variables.html">anti-pattern: global variables to share state ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/fault_tolerance/tasks.html#catching-application-level-failures">catching application-level failures ↗</a>
<a href="https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html">actor fault tolerance (Day 13's subject) ↗</a>
<a href="https://en.wikipedia.org/wiki/Actor_model">the actor model (Wikipedia) ↗</a>
</div>

## 2. What you will be seeing in the output

A sneak peek at the end of the day — the exact output you will produce,
with a plain-English note under each group of lines. Day 3's and Day 4's
lines still print first; today's lines follow them. If a note doesn't
fully land yet, that's fine; the rest of the day builds every piece:

```text
⋮ (Day 3's sixteen lines, then Day 4's eighteen, print first, unchanged)
a crew, one locked tab    -> the bill: $0, after about 300 ms
  after the cocktail      -> table 4's tab: $22
  after the beer          -> table 4's tab: $10
  after the lemonade      -> table 4's tab: $4
  still on the tab        -> $22
a bartender, one tab      -> the bill: $22, after about 600 ms
  after the cocktail      -> table 4's tab: $12
  after the beer          -> table 4's tab: $18
  after the lemonade      -> table 4's tab: $22
  still on the tab        -> $0
bartender-2's beer        -> ticket #2, table 7's tab: $6, after about 0 ms
bartender-1's cocktail    -> ticket #1, table 4's tab: $12, after about 300 ms
a cook splits bill #3     -> ticket #4, $6 each for 2 guests

thread 'bartender-2' (321555720) panicked at src/main.rs:532:9:
the glass slipped
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
another beer, table 7     -> task failed: the glass slipped
table 7's tab after that  -> $12
the crew pours, one tab   -> the bill: $22, after about 300 ms
  after the cocktail      -> table 4's tab: $12
  after the beer          -> table 4's tab: $18
  after the lemonade      -> table 4's tab: $22
  still on the tab        -> $0
```

- `a crew, one locked tab` and the four lines under it — a customer
  orders three items, then asks for the total. (The output's names —
  drinks, tabs, bills — come from the story, introduced below.) Each item
  is one job on a pool of four workers. It takes some time to make (the
  cocktail 300 ms, the beer 200, the lemonade 100), then adds its price
  to the customer's running total, which sits behind a lock. The fourth
  job reads the total out as the bill. Every job starts at once, so the quickest
  finishes first. The bill reads $0, the lemonade adds $4, the beer brings
  the total to $10, and the cocktail to $22. So $22 is still owed after the
  bill.

- `a bartender, one tab` and the four lines under it — the same evening,
  sent to one dedicated worker that owns the total: an actor. It runs the
  jobs one at a time, in the order they were sent. The total grows $12,
  $18, $22, in the order the guests asked. The bill is right, and nothing
  is left owed. But one worker makes every drink in turn: 600 ms instead
  of 300.

- The next three lines — two actors at once. The second one answers at
  0 ms while the first is still busy for 300 ms. Every result gets a
  number from one shared counter, so another job can read one of them.
  Here, a job reads one to split a bill in two.

- The five lines from the blank one to `table 7's tab after that` — one
  actor's job crashes on purpose. The crash report prints, the crash
  comes back as an error, and the actor keeps working. But the job had
  already added $6 before it crashed, and that change stayed: $12 owed
  for two drinks, only one served.

- `the crew pours, one tab` and the four lines under it — both ideas
  together. The pool makes the three drinks at the same time. The actor
  adds each price to the total, in the order the guests asked, waiting
  for each drink in turn. You get the right bill, in 300 ms.

## 3. Why we need it

**The story so far — remember [the restaurant](story.md):** a
[crew](story.md#crew) of [cooks](story.md#worker) takes
[order slips](story.md#task) from [one rail](story.md#shared-rail), and
whichever cook is free takes the next slip. Finished dishes land on
[the pass](story.md#the-pass) under their
[ticket numbers](story.md#ticket-number), and every pot sits once in
[the pantry](story.md#pantry).

Today's scene: **the bar.** Every cook in the crew is interchangeable,
and that is the crew's strength. But a guest at the bar runs a *tab*: a
running total that has to last from one drink to the next. For a tab, three
things about the crew stop working:

- **A slip has no memory.** A slip is a function that runs once and is
  gone, and the cook who cooked it may never see the next one. Anything
  that must outlive one slip (a tab, a cache, a model loaded once and
  used a thousand times) has to live somewhere else.

- **A tab behind a lock keeps every drink, but not their order.** The
  first place to try is one shared tab book that the cooks take turns
  writing in. You will watch the bill arrive before the drinks it should
  include.

- **Nobody answers for the tab.** In a shared book, anyone may write at
  any moment: any cook, even the waiter. A tab needs one keeper who
  takes requests about it ("add a beer", "what's owed?", "close it")
  one at a time, in the order they came.

Section A hires a **bartender**: a worker of their own, with a memory
(the tabs) and a rail of their own. Section B puts two bartenders next to
the kitchen and drops a glass. Section C lets the crew pour while the
bartender writes.

## 4. Mental model

Walk the bar, one stop per section:

- **The bartender (section A).** Yesterday's recap promised a worker
  with a memory. Meet the bartender. A cook with a memory needs a
  station nobody else uses, and that station is the bar. Like a lone
  cook, the bartender has a rail of their own and takes what is pinned
  there one at a time, oldest first. Unlike a cook, they keep things
  behind the bar: today, the tabs. Nobody else reaches over the bar: not
  the waiter, not a cook. To
  change a tab, or read one, you pin a **call** to the bartender's rail:
  "put a beer on table 4". Every call gets a ticket on the spot, and its
  answer lands on the pass like any dish.

- **Two bartenders (section B).** Each bartender has their own tabs and
  their own rail, so neither ever waits for the other. Both put their
  answers on the one pass, so any cook can read them. And when a glass
  slips mid-call, a note comes back instead of an answer. The bartender
  stays on shift, and whatever they had already written on the tab stays
  written.

- **The crew pours, the bartender writes (section C).** A call can carry
  a ticket for a drink the crew is still pouring. The bartender takes
  calls strictly in order, so at a call whose drink isn't up yet, they
  wait. Every call behind it waits too. That waiting is the price of
  the order.

```text
the waiter pins a call              it joins the bartender's rail, behind every earlier call
   │ the rail: strictly first come, first served
   ▼
the bartender takes the oldest call runs it with the tabs — nobody else ever touches them
   │ the call's answer
   ▼
the pass                            the answer lands under the call's ticket number
   │ the bartender walks back to the rail
   ▼
back to the top: the next call — until the rail is empty and closed
```

Read it top to bottom, as the bartender's evening. The left column is
where the call is, and the right column says what happens there. Compare it
with Day 4's crew, where several cooks met at one rail. Here, one
bartender owns one rail, so the order the calls were pinned in is the
order they run in.

## 5. Ray concepts introduced

- **Actors.** In Ray, `@ray.remote` on a class makes it an actor class.
  `Counter.remote()` starts a dedicated worker process that holds one
  instance of the class. Every method call, such as `counter.add.remote(1)`,
  runs on that worker, with that instance's state, and returns an
  `ObjectRef` at once. Ray's docs call an actor "essentially a stateful
  worker". Today's `Actor::start(name, &store, state)` starts the
  dedicated worker, and `actor.call(|state| …)` is the method call: a
  closure that runs with the state and returns a ticket.

- **Ordered calls.** A Ray actor of the plain kind (Ray's docs call it
  *synchronous, single-threaded*) runs the calls from one *submitter*
  (one caller, such as one program or one task) in the order they were
  submitted. A call does not start until every
  earlier call from the same submitter has finished, even if an earlier
  call is still waiting for its input. Today's actor keeps the same
  promise with one queue, the mailbox, so the order calls were sent in
  is the order they run in. Ray promises nothing about calls from
  *different* submitters. Ours has one mailbox for everyone, so it orders
  those too, by when they arrived.

- **State without locks.** The state lives on the actor's thread and
  nowhere else, so it needs no lock: there is never a second thread to
  race with. Ray's docs list an anti-pattern, global variables to share
  state between tasks, and give the same fix: put the state in an actor.
  (In Ray, today's first attempt would not even share the tab: tasks are
  separate processes, and each would write on a copy of its own.)

- **A failed call is an answer, and the actor survives.** When an actor
  method raises an exception, Ray stores the error as that call's result
  (`ray.get` raises it), and the actor keeps running. Nothing is rolled
  back: whatever the method changed before the exception stays changed.
  Today's actor does the same. What happens when the actor's *process*
  dies (restarts, and replaying the calls it lost) waits for Day 13.

- **Actors and tasks share one object store.** An actor call's result is
  an object like any other. You can pass its ref to a task, and you can pass a task's
  ref to an actor call. Today, actor tickets land on the
  same pass as task tickets and `put` values, with one numbering.

- **One honest difference.** Ray calls an actor's methods *by name*, and
  sends the arguments as bytes to another process. Ours is a thread in
  the same program, and a call is any Rust closure that takes the state.
  That is the most flexible kind of method, and one that can never leave this
  program. On Day 6, calls and results become bytes.

## 6. Build it, section by section

### Section A — the bartender keeps the tab

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://google.github.io/comprehensive-rust/std-traits/default.html"><code>Default</code> (<code>#[derive(Default)]</code>)</a>
<a href="https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html#method.or_default"><code>Entry::or_default</code> (std docs)</a>
</div>

Your Day 4 crew takes any slip, on any cook. So why touch it? Give it
something to remember, and watch. Table 4 orders three drinks, then asks
for the bill:

- a cocktail, $12, which takes 300 ms to pour,

- a beer, $6, 200 ms,

- a lemonade, $4, 100 ms.

Each drink is a slip. It pours, then writes its price on table 4's tab.
A fourth slip closes the tab and hands back the bill. A slip remembers
nothing, so the tab has to live outside the slips. Day 1 gave you the
tools for that: an `Arc` so every cook can hold the tab, and a `Mutex` so
they take turns writing in it.

Three edits to `src/main.rs`:

1. Replace `use std::sync::Arc;` (line 7) with these two lines:

   ```rust,ignore
   use std::collections::HashMap;
   use std::sync::{Arc, Mutex};
   ```

2. Add `a_tab_on_the_crew();` as the last line of `main`.

3. After the closing brace of `more_cooks_less_time` (around line 387 —
   edits 1 and 2 each added a line), add the evening, the tabs, a helper that prints the evening, and the
   scene:

   ```rust,ignore
   // ──────── new code starts here (around line 389) ────────
   /// Table 4's evening: three drinks, each with its price in dollars and how
   /// long it takes to pour, in milliseconds. The guests ask for the cocktail
   /// first, the lemonade last.
   const DRINKS: [(&str, u64, u64); 3] = [
       ("cocktail", 12, 300),
       ("beer", 6, 200),
       ("lemonade", 4, 100),
   ];

   /// Analogy: the tabs — what each table owes so far, kept from one drink to
   /// the next.
   ///
   /// A running total in dollars for every table with an open tab.
   #[derive(Default)]
   struct Tabs {
       by_table: HashMap<u32, u64>,
   }

   impl Tabs {
       /// Put `price` on `table`'s tab, opening it if needed; return the new total.
       fn add(&mut self, table: u32, price: u64) -> u64 {
           let tab = self.by_table.entry(table).or_default();
           *tab += price;
           *tab
       }

       /// Close `table`'s tab: return what it owes (nothing, if no tab is open).
       fn close(&mut self, table: u32) -> u64 {
           self.by_table.remove(&table).unwrap_or(0)
       }
   }

   /// Wait for the evening's three drinks and the bill, then print the bill,
   /// when it was all over, and the tab as each drink left it.
   fn tell_the_evening(label: &str, steps: &[Ticket<u64>], clock: Instant) {
       let seen: Vec<u64> = steps
           .iter()
           .map(|step| step.get().expect("no drink spills"))
           .collect();
       println!(
           "{label:<26}-> the bill: ${}, after about {} ms",
           seen[3],
           round_to_100(clock.elapsed())
       );
       for ((drink, _, _), tab) in DRINKS.iter().zip(&seen) {
           println!("  after the {drink:<14}-> table 4's tab: ${tab}");
       }
   }

   /// Table 4's evening on a crew of four, the tabs behind one lock: each slip
   /// pours its drink and writes it on the tab; the last slip asks for the bill.
   fn a_tab_on_the_crew() {
       let crew = Pool::start(4, &MemoryStore::new());
       let tabs = Arc::new(Mutex::new(Tabs::default()));
       let clock = Instant::now();
       let mut slips: Vec<Ticket<u64>> = DRINKS
           .iter()
           .map(|&(_, price, ms)| {
               let tabs = Arc::clone(&tabs);
               crew.submit(move || {
                   thread::sleep(Duration::from_millis(ms));
                   tabs.lock().unwrap().add(4, price)
               })
           })
           .collect();
       let for_the_bill = Arc::clone(&tabs);
       slips.push(crew.submit(move || for_the_bill.lock().unwrap().close(4)));
       tell_the_evening("a crew, one locked tab", &slips, clock);
       let left = tabs.lock().unwrap().close(4);
       println!("  still on the tab        -> ${left}");
   }
   // ──────── new code ends here ────────
   ```

A few details:

- `DRINKS` is an array of three *tuples*. Each one holds a name, a price in
  dollars, and a pouring time in milliseconds.

- `Tabs` keeps one running total per table, in a `HashMap` from table
  number to dollars. `#[derive(Default)]` writes `Tabs::default()` for
  you: a `Tabs` whose map is empty. In `add`, `entry(table).or_default()`
  finds the table's total, or starts one at 0 if the table has none yet,
  and hands back a `&mut u64` you can add to. `close` takes the table's
  total out of the map, and `unwrap_or(0)` turns "no tab open" into a bill of
  $0.

- `tell_the_evening` waits for the four tickets and prints the bill (the
  fourth result), then one line per drink. `DRINKS.iter().zip(&seen)`
  walks two lists side by side, pairing each drink with the total its slip
  saw, and `((drink, _, _), tab)` takes each pair apart, keeping only the
  drink's name.

- In `a_tab_on_the_crew`, every slip gets its own `Arc::clone` of the
  tab, as on Day 1. `tabs.lock().unwrap().add(4, price)` holds the lock
  only for that one statement. The pouring happens before it, unlocked.
  After the evening, the waiter locks the tab directly and closes it once
  more, to see what was left on it.

`cargo run`:

```text
⋮ (Day 3's sixteen lines, then Day 4's eighteen, print first, unchanged)
a crew, one locked tab    -> the bill: $0, after about 300 ms
  after the cocktail      -> table 4's tab: $22
  after the beer          -> table 4's tab: $10
  after the lemonade      -> table 4's tab: $4
  still on the tab        -> $22
```

Read the new lines one by one:

- `a crew, one locked tab -> the bill: $0, after about 300 ms` — the bill
  came back empty. All four slips went to four free cooks at once. The
  bill slip pours nothing, so it reached the tab first, when nothing was
  on it. The evening ended when the slowest slip, the cocktail, finished.

- `after the cocktail -> table 4's tab: $22` — the cocktail was asked for
  first, but its 300 ms of pouring made it the *last* to write.

- `after the beer -> table 4's tab: $10` and `after the lemonade -> … $4`
  — the lemonade wrote first ($4), then the beer ($4 + $6 = $10).

- `still on the tab -> $22` — every drink made it onto the tab, so the lock
  lost nothing. But the guests left with a bill of $0, and $22 stays owed.

The lock did its job. It keeps two cooks from writing at the same moment,
so no drink is lost. What it cannot keep is the *order*. On a crew, a
quick slip overtakes a slow one. On Day 4 that was the whole point, but
for a tab it is exactly wrong. **A lock keeps every write, not their
order.**

The fix is one keeper for the tabs: one thread that owns them, and a
rail of its own where requests wait, in the order they arrived. That keeper is
an **actor**. In the story, it is the bartender. The plan, in plain words:

- The actor's state (here, the tabs) *moves into* the actor's thread
  when it starts, and never comes out. No other thread can reach it.

- A request is a **call**: a function that takes the state, `&mut S`,
  and does one thing with it, such as "add a beer to table 4". Calls travel on a
  channel, as Day 1's jobs did. Actor systems such as Erlang and Akka
  call an actor's queue its **mailbox**, and so does our code. In the story,
  it is the bartender's own rail.

- The thread takes the calls one at a time, oldest first, and hands each
  one the state. Every call returns a ticket at once, and its answer lands
  on the pass, exactly like a slip's.

First, the actor needs the same two private tools of the worker's that
the crew needed on Day 4: [the cook's stub](story.md#stub) (`Promise`)
and `run_catching`. Make four one-word edits to `src/worker.rs`, each adding
`pub(crate)` at the start of a line. They are the same four as in Day 4's section
A, and this time they stay:

1. `fn run_catching<T, F>(task: F) -> Outcome` (around line 146).

2. `struct Promise {` (around line 162).

3. Inside `impl Promise`, `fn new(` (around line 169).

4. Inside `impl Promise`, `fn keep(` (around line 178).

Now create `src/actor.rs`:

```rust,ignore
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use crate::ticket::{MemoryStore, Ticket};
use crate::worker::{Promise, run_catching};

/// A call waiting in an actor's mailbox: a function the actor runs once, on
/// its own thread, with its state.
type Message<S> = Box<dyn FnOnce(&mut S) + Send + 'static>;

/// Analogy: the bartender — a worker with a memory (the tabs) and a rail of
/// their own (the mailbox), who takes calls one at a time, in the order they
/// came.
///
/// A dedicated thread that owns a value of type `S`, the actor's state, and
/// runs the calls sent to it strictly one at a time, in the order they were
/// sent. Only that thread ever touches the state, so the state needs no lock.
pub struct Actor<S> {
    mailbox: Option<Sender<Message<S>>>,
    handle: Option<JoinHandle<()>>,
    store: Arc<MemoryStore>,
}

impl<S: Send + 'static> Actor<S> {
    /// Start a named actor thread that owns `state` and puts the results of
    /// its calls in `store`.
    pub fn start(name: &str, store: &Arc<MemoryStore>, mut state: S) -> Actor<S> {
        let (mailbox, calls) = mpsc::channel::<Message<S>>();
        let handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                // One call at a time, oldest first, until the mailbox is closed and empty.
                for call in calls {
                    call(&mut state);
                }
            })
            .expect("failed to spawn actor thread");
        Actor {
            mailbox: Some(mailbox),
            handle: Some(handle),
            store: Arc::clone(store),
        }
    }

    /// Send `method` to the actor and return a ticket for its result at once.
    /// The actor runs it with its state, after every call sent before it.
    pub fn call<T, F>(&self, method: F) -> Ticket<T>
    where
        F: FnOnce(&mut S) -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let id = self.store.new_id();
        let promise = Promise::new(id, Arc::clone(&self.store));
        self.send(Box::new(move |state: &mut S| {
            promise.keep(run_catching(|| method(state)));
        }));
        Ticket::new(id, Arc::clone(&self.store))
    }

    /// Put `message` in the mailbox, behind every call already waiting there.
    fn send(&self, message: Message<S>) {
        // If the actor is gone the call is dropped unrun (its Promise reports it).
        let _ = self
            .mailbox
            .as_ref()
            .expect("actor is running")
            .send(message);
    }
}

impl<S> Drop for Actor<S> {
    fn drop(&mut self) {
        // Closing the mailbox lets the actor finish its calls, then stop.
        drop(self.mailbox.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
```

Read it piece by piece:

- `Message<S>` is one call waiting in the mailbox. Day 1's `Job` was "a
  function to call once, with nothing". A `Message<S>` is "a function to
  call once, with the state": `FnOnce(&mut S)`. `S` stands for the type
  of the state, whatever it is: `Tabs` today, a counter in the tests.

- `Actor<S>` has the same three parts as Day 1's `Worker`: the sending
  end of its channel (`mailbox`), its thread's handle, and the pass its
  answers land on.

- `start` takes the state as `mut state: S` and moves it into the thread
  with the `move` closure. The loop `for call in calls` takes one call at
  a time and hands it `&mut state`. That is the only way anything ever reaches
  the state. `mut` on the parameter is what allows that `&mut`.

- `impl<S: Send + 'static> Actor<S>` says which states an actor may own.
  `Send` means the state moves to another thread, once. `'static` means it borrows
  nothing from the code that started the actor. Notice what is missing:
  `Sync`, the mark of a value that several threads may *share*. The state
  never needs `Sync`, because it is never shared: only the actor's thread
  ever has it.

- `call` is the `submit` you know, with one change. It takes the same three steps (a
  fresh ticket number, a stub to fill, a ticket to hand back), but what
  goes into the mailbox is a `Message<S>`: a function that runs `method`
  with the state, inside `run_catching` so that a panic becomes an error
  (section B tries it).

- `send` puts a message in the mailbox. It is private. Outside this file,
  the only way into the mailbox is through `call`, which always makes a
  ticket.

- `Drop` is `Worker`'s, word for word. It closes the mailbox, lets the actor
  finish every call already in it, then waits for the thread to end.

Last, the scene. Four edits to `src/main.rs`:

1. Add `mod actor;` above `mod meter;` (line 1).

2. Add `use actor::Actor;` above `use pool::Pool;` (around line 7).

3. Add `a_bartender_keeps_the_tab();` as the last line of `main`.

4. After the closing brace of `a_tab_on_the_crew` (around line 462), add
   the same evening, now at the bar:

   ```rust,ignore
   // ──────── new code starts here (around line 464) ────────
   /// Table 4's evening with a bartender: an actor who owns the tabs and pours
   /// every drink, one call at a time.
   fn a_bartender_keeps_the_tab() {
       let bartender = Actor::start("bartender", &MemoryStore::new(), Tabs::default());
       let clock = Instant::now();
       let mut calls: Vec<Ticket<u64>> = DRINKS
           .iter()
           .map(|&(_, price, ms)| {
               bartender.call(move |tabs: &mut Tabs| {
                   thread::sleep(Duration::from_millis(ms));
                   tabs.add(4, price)
               })
           })
           .collect();
       calls.push(bartender.call(|tabs: &mut Tabs| tabs.close(4)));
       tell_the_evening("a bartender, one tab", &calls, clock);
       let left = bartender.call(|tabs: &mut Tabs| tabs.close(4));
       println!(
           "  still on the tab        -> ${}",
           left.get().expect("no drink spills")
       );
   }
   // ──────── new code ends here ────────
   ```

Each drink is now a call to the bartender, written as a closure:
`move |tabs: &mut Tabs| { … }` is today's version of Ray's
`bartender.add.remote(4, price)`. The waiter never touches the tabs.
Even "what is still on the tab?" at the end is a call.

`cargo run`:

```text
⋮ (Day 3's sixteen lines, then Day 4's eighteen, print first, unchanged)
a crew, one locked tab    -> the bill: $0, after about 300 ms
  after the cocktail      -> table 4's tab: $22
  after the beer          -> table 4's tab: $10
  after the lemonade      -> table 4's tab: $4
  still on the tab        -> $22
──── everything above: the first run's lines, unchanged ────
a bartender, one tab      -> the bill: $22, after about 600 ms
  after the cocktail      -> table 4's tab: $12
  after the beer          -> table 4's tab: $18
  after the lemonade      -> table 4's tab: $22
  still on the tab        -> $0
```

Read the new lines one by one:

- `a bartender, one tab -> the bill: $22, after about 600 ms` — the
  right bill. The bartender ran the four calls in the order they were
  sent, so the bill came last. But the bartender poured every drink
  alone, one after another: 300 + 200 + 100 = 600 ms, twice the crew's
  time.

- `after the cocktail -> $12`, `after the beer -> $18`,
  `after the lemonade -> $22` — the tab grew in the order the guests
  asked, each call seeing the tab exactly as the call before it left it.
  `call` put each one in the mailbox, and the loop in `Actor::start` ran
  them, oldest first.

- `still on the tab -> $0` — nothing left owed.

The bill is right, but the evening is slow. Section C fixes the slow part. First, five tests pin
the actor. Append a test module to the end of `src/actor.rs`, after the
closing brace of `impl Drop` (around line 80):

```rust,ignore
// ──────── new code starts here (around line 82) ────────
#[cfg(test)]
mod tests {
    use super::Actor;
    use crate::ticket::{MemoryStore, Ticket};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;

    /// The name of the thread running this code.
    fn thread_name() -> String {
        thread::current().name().unwrap_or("?").to_string()
    }

    #[test]
    fn a_call_returns_its_result_on_a_ticket() {
        let counter = Actor::start("counter", &MemoryStore::new(), 40_u64);
        let ticket = counter.call(|count: &mut u64| *count + 2);
        assert_eq!(ticket.get().unwrap(), 42);
    }

    #[test]
    fn the_state_outlives_every_call() {
        let counter = Actor::start("counter", &MemoryStore::new(), 0_u64);
        let counts: Vec<Ticket<u64>> = (0..3)
            .map(|_| {
                counter.call(|count: &mut u64| {
                    *count += 1;
                    *count
                })
            })
            .collect();
        let seen: Vec<u64> = counts.iter().map(|ticket| ticket.get().unwrap()).collect();
        assert_eq!(seen, [1, 2, 3]);
    }

    #[test]
    fn calls_run_one_at_a_time_in_the_order_sent() {
        let log = Actor::start("logger", &MemoryStore::new(), Vec::<String>::new());
        for n in 1..=100 {
            log.call(move |lines: &mut Vec<String>| {
                lines.push(format!("{n} on {}", thread_name()))
            });
        }
        let lines = log
            .call(|lines: &mut Vec<String>| lines.clone())
            .get()
            .unwrap();
        let expected: Vec<String> = (1..=100).map(|n| format!("{n} on logger")).collect();
        assert_eq!(lines, expected);
    }

    #[test]
    fn call_returns_before_the_method_runs() {
        let counter = Actor::start("counter", &MemoryStore::new(), 0_u64);
        let (open_gate, gate) = mpsc::channel::<()>();
        // The actor cannot finish this call until we open the gate...
        let stuck = counter.call(move |count: &mut u64| {
            gate.recv().unwrap();
            *count += 1;
        });
        // ...yet we already hold tickets for it and for the call behind it.
        let after = counter.call(|count: &mut u64| *count);
        open_gate.send(()).unwrap();
        stuck.get().unwrap();
        assert_eq!(after.get().unwrap(), 1);
    }

    #[test]
    fn dropping_the_actor_finishes_its_mailbox() {
        let done = Arc::new(AtomicUsize::new(0));
        let counter = Actor::start("counter", &MemoryStore::new(), 0_u64);
        for _ in 0..10 {
            let done = Arc::clone(&done);
            counter.call(move |count: &mut u64| {
                *count += 1;
                done.fetch_add(1, Ordering::SeqCst);
            });
        }
        drop(counter);
        assert_eq!(done.load(Ordering::SeqCst), 10);
    }
}
// ──────── new code ends here ────────
```

None of them sleeps. The one that needs timing uses a gate, as on Day 1:

- `a_call_returns_its_result_on_a_ticket` — an actor's state is a
  plain number, 40. A call adds 2, and the ticket brings back 42.

- `the_state_outlives_every_call` — three calls each add 1 and report the
  count: 1, 2, 3. Each call saw what the call before it left.

- `calls_run_one_at_a_time_in_the_order_sent` — a hundred calls each
  write their number and their thread's name into a list the actor owns.
  The list comes back as 1 to 100, in order, every one written by the
  actor's own thread, `logger`.

- `call_returns_before_the_method_runs` — the first call waits at a gate
  that only the test can open. The test holds tickets for it *and* for
  the call behind it before opening the gate. So `call` never waits for
  the actor. Once the gate opens, the second call sees the count the
  first one left: 1.

- `dropping_the_actor_finishes_its_mailbox` — ten calls, then the test drops the
  actor, and all ten have run.

```bash
cargo test
```

```text
running 53 tests
test actor::tests::call_returns_before_the_method_runs ... ok
test actor::tests::a_call_returns_its_result_on_a_ticket ... ok
test actor::tests::the_state_outlives_every_call ... ok
test actor::tests::dropping_the_actor_finishes_its_mailbox ... ok
test actor::tests::calls_run_one_at_a_time_in_the_order_sent ... ok
⋮ (the 48 earlier tests, all ok)

test result: ok. 53 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

All 48 earlier tests still pass, plus the five new ones.

**Section checkpoint:** `./scripts/compare.sh v1-day-05a`.

### Section B — two bartenders, one pass

<div class="rust-concepts"><span>Rust used here:</span>
<a href="https://doc.rust-lang.org/std/option/enum.Option.html#method.copied"><code>Option::copied</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/result/enum.Result.html#method.expect_err"><code>Result::expect_err</code> (std docs)</a>
<a href="https://doc.rust-lang.org/std/panic/trait.UnwindSafe.html"><code>UnwindSafe</code> (std docs)</a>
</div>

Your bartender keeps one tab right. A real bar has more than one
bartender, and the rest of the kitchen has to use what they hand out.
One scene answers three questions:

- **Does one bartender wait for another?** Bartender 1 pours a slow
  cocktail for table 4, and bartender 2 pours a quick beer for table 7.

- **Can a cook use a bartender's answer?** Bartender 1 closes table 4's
  tab, and a cook splits the bill between two guests. The cook's slip reads
  the bill's ticket.

- **What happens when a call panics?** Bartender 2 starts another beer
  for table 7. They put it on the tab, then drop the glass.

Three edits to `src/main.rs`:

1. Add `two_bartenders_one_pass();` as the last line of `main`.

2. Inside `impl Tabs`, after the closing brace of `close` (around line
   422), add a way to read a tab without closing it:

   ```rust,ignore
       // ──────── new code starts here (around line 424) ────────
       /// What `table` owes so far, leaving its tab open.
       fn owed(&self, table: u32) -> u64 {
           self.by_table.get(&table).copied().unwrap_or(0)
       }
       // ──────── new code ends here ────────
   ```

3. After the closing brace of `a_bartender_keeps_the_tab` (around line
   491), add the scene:

   ```rust,ignore
   // ──────── new code starts here (around line 493) ────────
   /// Two bartenders on one pass: each keeps tabs of their own, neither waits
   /// for the other, a cook reads what one of them hands out, and a glass slips.
   fn two_bartenders_one_pass() {
       let pass = MemoryStore::new();
       let crew = Pool::start(2, &pass);
       let bartenders = [
           Actor::start("bartender-1", &pass, Tabs::default()),
           Actor::start("bartender-2", &pass, Tabs::default()),
       ];
       let clock = Instant::now();
       let cocktail = bartenders[0].call(|tabs: &mut Tabs| {
           thread::sleep(Duration::from_millis(300));
           tabs.add(4, 12)
       });
       let beer = bartenders[1].call(|tabs: &mut Tabs| tabs.add(7, 6));
       for (who, table, ticket) in [
           ("bartender-2's beer", 7, &beer),
           ("bartender-1's cocktail", 4, &cocktail),
       ] {
           let tab = ticket.get().expect("no drink spills");
           println!(
               "{who:<26}-> ticket {}, table {table}'s tab: ${tab}, after about {} ms",
               ticket.id(),
               round_to_100(clock.elapsed())
           );
       }

       let bill = bartenders[0].call(|tabs: &mut Tabs| tabs.close(4));
       let each = crew.submit_reading(&bill, |bill: &u64| bill / 2);
       println!(
           "a cook splits bill {}     -> ticket {}, ${} each for 2 guests",
           bill.id(),
           each.id(),
           each.get().expect("splitting never burns")
       );

       let slipped = bartenders[1].call(|tabs: &mut Tabs| -> u64 {
           tabs.add(7, 6);
           panic!("the glass slipped")
       });
       let error = slipped.get().expect_err("the glass always slips");
       println!("another beer, table 7     -> {error}");
       let owed = bartenders[1].call(|tabs: &mut Tabs| tabs.owed(7));
       println!(
           "table 7's tab after that  -> ${}",
           owed.get().expect("reading a tab never burns")
       );
   }
   // ──────── new code ends here ────────
   ```

A few details:

- `owed` uses `get`, which leaves the tab open, where `close` used
  `remove`. `get` hands back an `Option<&u64>`, a reference into the
  map, and `.copied()` turns it into an `Option<u64>`, a copy of the
  number, so `unwrap_or(0)` can supply a plain `0` when there is no tab.

- Both bartenders and the crew share one pass, so all their tickets come
  from one numbering. The `for` loop prints the beer first and then the
  cocktail, each with its ticket number and the time it was ready.

- `crew.submit_reading(&bill, …)` is Day 3's slip that reads a pot
  [in place](story.md#read-in-place), given a bartender's ticket instead of a cook's. The difference
  means nothing to the slip: a ticket is a ticket.

- The slipped glass is a call that returns `u64` (the new total), but it
  panics before it gets there. `expect_err` is `expect` the other way
  round. It hands back the error, and it would crash if the call had
  somehow succeeded.

`cargo run`:

```text
⋮ (Day 3's sixteen lines, Day 4's eighteen, and section A's ten print first, unchanged)
bartender-2's beer        -> ticket #2, table 7's tab: $6, after about 0 ms
bartender-1's cocktail    -> ticket #1, table 4's tab: $12, after about 300 ms
a cook splits bill #3     -> ticket #4, $6 each for 2 guests

thread 'bartender-2' (321549000) panicked at src/main.rs:531:9:
the glass slipped
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
another beer, table 7     -> task failed: the glass slipped
table 7's tab after that  -> $12
```

Read the new lines one by one:

- `bartender-2's beer -> ticket #2, table 7's tab: $6, after about 0 ms`
  — bartender 2 answered at once, while bartender 1 was still pouring.
  Each `Actor` is its own thread with its own mailbox, so nothing makes
  one wait for the other. The beer's ticket is `#2`: the cocktail's call
  took `#1` a moment earlier, on the same pass.

- `bartender-1's cocktail -> ticket #1, table 4's tab: $12, after about
  300 ms` — the cocktail landed when its 300 ms were up.

- `a cook splits bill #3 -> ticket #4, $6 each for 2 guests` — bartender
  1's bill ($12) landed on the pass as `#3`. A cook's slip read it in
  place and halved it. Tasks and actors share one pass and one numbering.

- The blank line and the three lines from `thread 'bartender-2' … panicked`
  — the glass slips. As on Day 1, Rust prints its standard crash report.
  This time the thread's name is the actor's, `bartender-2`, and the line
  number points at the `panic!` in `two_bartenders_one_pass`.

- `another beer, table 7 -> task failed: the glass slipped` — the panic
  came back as a [burnt-dish note](story.md#task-error) on the call's
  ticket, because `run_catching` in `call` caught it.

- `table 7's tab after that -> $12` — two things at once. Bartender 2 is
  still on shift, because this call, sent after the panic, ran. And the tab holds
  *two* beers, because the call put the second beer on the tab, then dropped the
  glass. Table 7 now owes for a beer it never got.

Is keeping the bartender on shift the right choice? The alternative is
to let a panic end the actor, and with it every tab of every table, all for
one dropped glass. Ray chooses survival too. An exception in an actor
method becomes that call's error, and the actor keeps serving. So does
ours.

But survival has a catch, and Rust has a name for it. Three short steps:

1. **Unwinding.** When a call panics, Rust *unwinds*. It walks back out
   of the crashed function, step by step, until `catch_unwind` in
   `run_catching` stops it. Whatever the function was in the middle of
   doing stays exactly as far along as it got.

2. **`UnwindSafe`.** Rust labels the values that are safe to look at
   after such a crash `UnwindSafe`. A `&mut` reference does not get the
   label, because a change that was half finished when the panic hit can leave
   the value behind it half-changed. Leave out the wrapper below, and the
   compiler refuses `call`, saying "`&mut S` may not be safely
   transferred across an unwind boundary".

3. **`AssertUnwindSafe`.** This wrapper, which every task has run inside
   since Day 1, tells the compiler "trust me, nothing half-changed will
   be seen". On Day 1 that promise was true: "Our tasks share nothing
   with the worker, so the promise holds." For a call, it is false, because the
   state outlives the call. Table 7's tab is that promise breaking, in
   public.

So there are two honest ways to handle a panic mid-call. The strict way
refuses every later call after a panic: "this tab may be half-written".
(Rust's `Mutex` does this, and it calls it
[poisoning](https://doc.rust-lang.org/std/sync/struct.Mutex.html#poisoning).)
Ray takes the relaxed way, and so do we. The actor carries on, and
nothing is rolled back. That puts a rule on whoever writes calls:
**check first, change last**. A call that might fail should do all its
failing before it touches the state.

Five tests pin it. Three edits to `src/actor.rs`'s `mod tests`:

1. Replace `use crate::ticket::{MemoryStore, Ticket};` (line 85) with
   these three lines. They bring in the crew, [`put`](story.md#put) and
   [`wait`](story.md#wait), and [the `Submit` trait](story.md#submit-trait)
   for the crew's methods:

   ```rust,ignore
       use crate::pool::Pool;
       use crate::ticket::{MemoryStore, Ticket, put, wait};
       use crate::worker::Submit;
   ```

2. Add `use std::time::Duration;` below `use std::thread;` (around line 90).

3. Add five tests at the end of `mod tests`, before the file's final
   closing `}`:

   ```rust,ignore
   mod tests {
       ⋮
       // ──────── new code starts here (around line 167) ────────
       #[test]
       fn two_actors_work_at_the_same_time() {
           let store = MemoryStore::new();
           let one = Actor::start("one", &store, 0_u64);
           let two = Actor::start("two", &store, 0_u64);
           let (open_gate, gate) = mpsc::channel::<()>();
           let stuck = one.call(move |_: &mut u64| gate.recv().unwrap());
           let quick = two.call(|count: &mut u64| *count + 1);
           // `one` is stuck behind the gate; only `two` can answer.
           let (ready_now, _) = wait(vec![quick], 1, Duration::from_secs(10));
           open_gate.send(()).unwrap();
           assert_eq!(ready_now.len(), 1, "the other actor answered");
           stuck.get().unwrap();
       }

       #[test]
       fn actor_tickets_share_the_pass_with_tasks_and_puts() {
           let store = MemoryStore::new();
           let crew = Pool::start(2, &store);
           let counter = Actor::start("counter", &store, 0_u64);
           let ids = [
               crew.submit(|| 1_u64).id().to_string(),
               counter.call(|count: &mut u64| *count).id().to_string(),
               put(&store, 3_u64).id().to_string(),
           ];
           assert_eq!(ids, ["#1", "#2", "#3"]);
       }

       #[test]
       fn a_task_reads_an_actors_result() {
           let store = MemoryStore::new();
           let crew = Pool::start(2, &store);
           let counter = Actor::start("counter", &store, 20_u64);
           let count = counter.call(|count: &mut u64| {
               *count += 1;
               *count
           });
           let doubled = crew.submit_reading(&count, |count: &u64| count * 2);
           assert_eq!(doubled.get().unwrap(), 42);
       }

       #[test]
       fn a_panicking_call_becomes_an_error_and_the_actor_carries_on() {
           let counter = Actor::start("counter", &MemoryStore::new(), 7_u64);
           let burnt = counter.call(|_: &mut u64| -> u64 { panic!("the glass slipped") });
           assert_eq!(
               burnt.get().unwrap_err().to_string(),
               "task failed: the glass slipped"
           );
           assert_eq!(counter.call(|count: &mut u64| *count).get().unwrap(), 7);
       }

       #[test]
       fn a_panic_keeps_what_the_call_changed_before_it() {
           let counter = Actor::start("counter", &MemoryStore::new(), 0_u64);
           let burnt = counter.call(|count: &mut u64| -> u64 {
               *count += 1;
               panic!("halfway through")
           });
           assert!(burnt.get().is_err());
           // Nothing rolls back: the change made before the panic stays.
           assert_eq!(counter.call(|count: &mut u64| *count).get().unwrap(), 1);
       }
       // ──────── new code ends here ────────
   }
   ```

What each one proves:

- `two_actors_work_at_the_same_time` jams actor `one` behind a gate. A
  call to actor `two` can then be answered only if `two` is a separate
  thread, so `wait` sees it ready. The generous 10 s limit only matters
  if the actors are broken.

- `actor_tickets_share_the_pass_with_tasks_and_puts` — a crew slip, an
  actor call, and a `put`, on one store: `#1`, `#2`, `#3`.

- `a_task_reads_an_actors_result` — an actor counts to 21, and a crew
  slip reads that ticket in place and doubles it: 42.

- `a_panicking_call_becomes_an_error_and_the_actor_carries_on` — the
  error arrives with its message, and the next call still sees the state,
  7, untouched.

- `a_panic_keeps_what_the_call_changed_before_it` — pins today's
  decision, so nobody changes it by accident. The count was raised to 1
  before the panic, and it stays 1.

```bash
cargo test
```

```text
running 58 tests
test actor::tests::two_actors_work_at_the_same_time ... ok
test actor::tests::actor_tickets_share_the_pass_with_tasks_and_puts ... ok
test actor::tests::a_task_reads_an_actors_result ... ok
test actor::tests::a_panicking_call_becomes_an_error_and_the_actor_carries_on ... ok
test actor::tests::a_panic_keeps_what_the_call_changed_before_it ... ok
⋮ (the 53 earlier tests, all ok)

test result: ok. 58 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

All 53 earlier tests pass, plus the five new ones.

**Section checkpoint:** `./scripts/compare.sh v1-day-05b`.

### Section C — the crew pours, the bartender writes

<div class="rust-concepts"><span>Rust used here:</span> nothing new —
`call_reading` is built from the same pieces as Day 3's `submit_reading`
and today's `call`.</div>

Put section A's two evenings side by side. The crew took 300 ms and got
the bill wrong. The bartender got it right and took 600 ms, because the
bartender poured every drink alone. But pouring needs no memory: any cook
can do it, side by side, like Day 4's crew. Only *writing on the tab*
needs the bartender. So let the crew pour, and the bartender write.

The waiter gets a ticket for each drink the moment its slip goes to the
crew. What the bartender needs is a call that carries such a
ticket: "when this drink is up, put its price on table 4's tab."

Day 2 solved a problem shaped like this one. A
[held slip](story.md#held-slip) waits clipped to the pass, off the rail,
and joins the rail only when its input lands, so it never blocks a cook.
A call cannot step aside like that. If it waited off the rail, the calls
behind it would overtake it, and overtaking is exactly section A's bug.
So a call that carries a ticket joins the bartender's rail at once, in
its place.
When its turn comes, the bartender looks for the drink, and if it is not
up yet, waits for it right there. Every call behind it waits too. That
is the price of keeping the order.

Three edits, in order. First `src/actor.rs`:

1. Inside `impl Actor`, after the closing brace of `call` (around line
   59), add:

   ```rust,ignore
       // ──────── new code starts here (around line 61) ────────
       /// Like `call`, but `method` also reads the result behind `input`, in
       /// place. The call joins the mailbox at once. When its turn comes, the
       /// actor waits there for `input` to land, and every later call waits with
       /// it — so calls still run in the order they were sent. If `input` failed,
       /// `method` never runs and its ticket carries the same error.
       pub fn call_reading<A, T, F>(&self, input: &Ticket<A>, method: F) -> Ticket<T>
       where
           A: Send + Sync + 'static,
           F: FnOnce(&mut S, &A) -> T + Send + 'static,
           T: Send + Sync + 'static,
       {
           let id = self.store.new_id();
           let promise = Promise::new(id, Arc::clone(&self.store));
           let input_copy = input.clone();
           self.send(Box::new(move |state: &mut S| {
               // Blocks until the input lands, holding up the calls behind this one.
               let outcome = match input_copy.read() {
                   Ok(value) => run_catching(|| method(state, &value)),
                   Err(error) => Err(error),
               };
               promise.keep(outcome);
           }));
           Ticket::new(id, Arc::clone(&self.store))
       }
       // ──────── new code ends here ────────
   ```

It is Day 3's `submit_reading`, reshaped for an actor:

- `input_copy` is a copy of the input's ticket, made to travel inside the
  message.

- `input_copy.read()` runs on the actor's thread when the call's turn
  comes. If the input has not landed, `read` blocks until it does, and
  that block *is* the waiting in line.

- `method(state, &value)` gets both the state and the input, read in
  place, as `&A`, never copied. Like `call`, it runs inside
  `run_catching`.

- If the input failed, `method` never runs, and the call's ticket carries
  the input's error, message intact. The burnt-dish note
  [travels down the chain](story.md#error-propagation), as on Day 2.

Then `src/main.rs`:

2. Add `the_crew_pours_the_bartender_writes();` as the last line of
   `main`.

3. After the closing brace of `two_bartenders_one_pass` (around line
   541), add:

   ```rust,ignore
   // ──────── new code starts here (around line 543) ────────
   /// Table 4's evening once more: the crew pours the drinks side by side, and
   /// the bartender writes each one on the tab, in the order the guests asked.
   fn the_crew_pours_the_bartender_writes() {
       let pass = MemoryStore::new();
       let crew = Pool::start(4, &pass);
       let bartender = Actor::start("bartender", &pass, Tabs::default());
       let clock = Instant::now();
       let mut calls: Vec<Ticket<u64>> = DRINKS
           .iter()
           .map(|&(_, price, ms)| {
               let drink = crew.submit(move || {
                   thread::sleep(Duration::from_millis(ms));
                   price
               });
               bartender.call_reading(&drink, |tabs: &mut Tabs, price: &u64| tabs.add(4, *price))
           })
           .collect();
       calls.push(bartender.call(|tabs: &mut Tabs| tabs.close(4)));
       tell_the_evening("the crew pours, one tab", &calls, clock);
       let left = bartender.call(|tabs: &mut Tabs| tabs.close(4));
       println!(
           "  still on the tab        -> ${}",
           left.get().expect("no drink spills")
       );
   }
   // ──────── new code ends here ────────
   ```

For each drink, the waiter hands the crew a slip that pours it, and the
bartender a call that reads the slip's ticket. All six (three pouring
slips and three calls) go out at once, in the order the guests asked
for them, and the bill's call follows.

`cargo run`:

```text
⋮ (every earlier line prints first, unchanged — the last nine are section B's)
bartender-2's beer        -> ticket #2, table 7's tab: $6, after about 0 ms
bartender-1's cocktail    -> ticket #1, table 4's tab: $12, after about 300 ms
a cook splits bill #3     -> ticket #4, $6 each for 2 guests

thread 'bartender-2' (321555720) panicked at src/main.rs:532:9:
the glass slipped
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
another beer, table 7     -> task failed: the glass slipped
table 7's tab after that  -> $12
──── everything above: section B's lines, unchanged ────
the crew pours, one tab   -> the bill: $22, after about 300 ms
  after the cocktail      -> table 4's tab: $12
  after the beer          -> table 4's tab: $18
  after the lemonade      -> table 4's tab: $22
  still on the tab        -> $0
```

Read the new lines one by one:

- `the crew pours, one tab -> the bill: $22, after about 300 ms` — the
  right bill, as fast as the crew. Three cooks poured the three drinks
  side by side, so the evening took as long as the slowest one, the
  cocktail.

- `after the cocktail -> $12`, `after the beer -> $18`,
  `after the lemonade -> $22` — written in the order the guests asked,
  even though the lemonade was poured first. The bartender reached the
  cocktail's call first and waited there, in `call_reading`'s `read`,
  for 300 ms. By then the beer and the lemonade were long up, so their
  calls ran at once.

- `still on the tab -> $0` — nothing left owed.

That wait has a cost you can see in the code, if not in this output.
While the bartender waits for the cocktail, *no* call gets through, not
even one for another table. Ray's actors pay the same price for calls
from one submitter.

Three tests pin it. Add them at the end of `src/actor.rs`'s
`mod tests`, before the file's final closing `}`:

```rust,ignore
mod tests {
    ⋮
    // ──────── new code starts here (around line 256) ────────
    #[test]
    fn calls_reading_tickets_still_run_in_the_order_sent() {
        let store = MemoryStore::new();
        let crew = Pool::start(2, &store);
        let log = Actor::start("logger", &store, Vec::<String>::new());
        let (open_first, first_gate) = mpsc::channel::<()>();
        let (open_second, second_gate) = mpsc::channel::<()>();
        let first = crew.submit(move || {
            first_gate.recv().unwrap();
            "first".to_string()
        });
        let second = crew.submit(move || {
            second_gate.recv().unwrap();
            "second".to_string()
        });
        for input in [&first, &second] {
            log.call_reading(input, |lines: &mut Vec<String>, dish: &String| {
                lines.push(dish.clone())
            });
        }
        log.call(|lines: &mut Vec<String>| lines.push("later".to_string()));
        // The second input lands first, then the first one.
        open_second.send(()).unwrap();
        second.get().unwrap();
        open_first.send(()).unwrap();
        let lines = log.call(|lines: &mut Vec<String>| lines.clone());
        assert_eq!(lines.get().unwrap(), ["first", "second", "later"]);
    }

    #[test]
    fn an_actors_own_ticket_can_feed_its_next_call() {
        let counter = Actor::start("counter", &MemoryStore::new(), 1_u64);
        let first = counter.call(|count: &mut u64| {
            *count *= 10;
            *count
        });
        let next = counter.call_reading(&first, |count: &mut u64, seen: &u64| *count + seen);
        assert_eq!(next.get().unwrap(), 20);
    }

    #[test]
    fn a_failed_input_skips_the_call_and_passes_the_error_on() {
        let store = MemoryStore::new();
        let crew = Pool::start(1, &store);
        let counter = Actor::start("counter", &store, 0_u64);
        let burnt = crew.submit(|| -> u64 { panic!("the oven caught fire") });
        let skipped = counter.call_reading(&burnt, |count: &mut u64, price: &u64| {
            *count += price;
            *count
        });
        assert_eq!(
            skipped.get().unwrap_err().to_string(),
            "task failed: the oven caught fire"
        );
        let count = counter.call(|count: &mut u64| *count);
        assert_eq!(count.get().unwrap(), 0, "the skipped call never ran");
    }
    // ──────── new code ends here ────────
}
```

- `calls_reading_tickets_still_run_in_the_order_sent` — two crew slips,
  each behind a gate, feed two calls that log their input, and a plain call,
  `later`, follows. The test opens the *second* gate first and waits for
  that dish to land, then opens the first. The log still reads
  `first`, `second`, `later`: the order the calls were sent, not the
  order their inputs landed.

- `an_actors_own_ticket_can_feed_its_next_call` — an actor's call can
  read the ticket of an earlier call to the same actor. That earlier call
  is ahead in the line, so its answer is always there in time, and there is no
  deadlock. (The count becomes 10, and the next call adds the 10 it
  read: 20.)

- `a_failed_input_skips_the_call_and_passes_the_error_on` — a burnt input
  leaves its note on the call's ticket, and the count shows the call
  never ran.

```bash
cargo test
```

```text
running 61 tests
test actor::tests::an_actors_own_ticket_can_feed_its_next_call ... ok
test actor::tests::calls_reading_tickets_still_run_in_the_order_sent ... ok
test actor::tests::a_failed_input_skips_the_call_and_passes_the_error_on ... ok
⋮ (the 58 earlier tests, all ok)

test result: ok. 61 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

**Section checkpoint:** `./scripts/compare.sh v1-day-05c` — which is also
the finished day.

## 7. Run it

```bash
cargo run
```

## 8. Expected result

```text
⋮ (Day 3's sixteen lines, then Day 4's eighteen, print first, unchanged)
a crew, one locked tab    -> the bill: $0, after about 300 ms
  after the cocktail      -> table 4's tab: $22
  after the beer          -> table 4's tab: $10
  after the lemonade      -> table 4's tab: $4
  still on the tab        -> $22
a bartender, one tab      -> the bill: $22, after about 600 ms
  after the cocktail      -> table 4's tab: $12
  after the beer          -> table 4's tab: $18
  after the lemonade      -> table 4's tab: $22
  still on the tab        -> $0
bartender-2's beer        -> ticket #2, table 7's tab: $6, after about 0 ms
bartender-1's cocktail    -> ticket #1, table 4's tab: $12, after about 300 ms
a cook splits bill #3     -> ticket #4, $6 each for 2 guests

thread 'bartender-2' (321555720) panicked at src/main.rs:532:9:
the glass slipped
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
another beer, table 7     -> task failed: the glass slipped
table 7's tab after that  -> $12
the crew pours, one tab   -> the bill: $22, after about 300 ms
  after the cocktail      -> table 4's tab: $12
  after the beer          -> table 4's tab: $18
  after the lemonade      -> table 4's tab: $22
  still on the tab        -> $0
```

The whole evening at the bar, one stretch at a time:

- The first five of today's lines — table 4's tab on a crew, behind a
  lock: every drink written, the bill wrong, $22 left owed.
  (`a_tab_on_the_crew`, `tell_the_evening`.)

- The next five — the same evening at the bar: the right bill, the
  drinks written in the order the guests asked, in twice the time.
  (`Actor::start`, `Actor::call`, `a_bartender_keeps_the_tab`.)

- The next nine — two bartenders who never wait for each other, a cook
  who reads a bartender's ticket, and a slipped glass: the bartender
  stays, and the second beer stays on the tab. (`two_bartenders_one_pass`.)

- The last five — the crew pours, the bartender writes: the right bill,
  at the crew's speed. (`Actor::call_reading`,
  `the_crew_pours_the_bartender_writes`.)

What varies between runs: Day 4's millisecond figures and speedups, the
thread id in brackets in the crash report, and that report's line number
if your `main` is laid out differently. Today's other lines round to the
nearest 100 ms and stay the same on every run. (One honest caveat, as on
Days 2 to 4: today's scenes rely on `thread::sleep` for pouring times, and
the crew's wrong bill relies on 100 ms gaps between the drinks. The tests
rely on gates instead.)

## 9. Test it

Sixty-one tests now pin the engine — Day 4's 48, plus 13 new ones today:
the actor itself (section A, 5 tests), actors side by side, on the pass,
and panicking (section B, 5), and calls that wait for a ticket (section
C, 3):

```bash
cargo test
```

It runs the same 61 tests as at the end of section C, in some order, and
ends:

```text
test result: ok. 61 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

(All 61 are unit tests; the program still takes no command-line input,
so there is nothing for a command-line test to drive.)

## 10. Break it deliberately

Here is an improvement you might well be tempted by: the bartender
stands idle while waiting for the cocktail. So why not let the call wait
the way Day 2's held slip does, off the rail, and join the bartender's
rail only once its drink is up? `Ticket::when_ready` does exactly that. In
`call_reading` in `src/actor.rs` (around lines 75–82), turn the
`self.send(…)` into a message that is sent only when the input lands:

```diff
         let id = self.store.new_id();
         let promise = Promise::new(id, Arc::clone(&self.store));
         let input_copy = input.clone();
-        self.send(Box::new(move |state: &mut S| {
+        let message: Message<S> = Box::new(move |state: &mut S| {
             // Blocks until the input lands, holding up the calls behind this one.
             let outcome = match input_copy.read() {
                 Ok(value) => run_catching(|| method(state, &value)),
                 Err(error) => Err(error),
             };
             promise.keep(outcome);
+        });
+        let mailbox = self.mailbox.as_ref().expect("actor is running").clone();
+        input.when_ready(Box::new(move || {
+            let _ = mailbox.send(message);
         }));
         Ticket::new(id, Arc::clone(&self.store))
     }
```

It compiles without a warning. `cargo run`, today's last scene:

```text
⋮ (every line before the last scene prints as before)
the crew pours, one tab   -> the bill: $0, after about 300 ms
  after the cocktail      -> table 4's tab: $22
  after the beer          -> table 4's tab: $10
  after the lemonade      -> table 4's tab: $4
  still on the tab        -> $22
```

Read it as a list of symptoms:

- `the bill: $0` — the bill's call never waited for anything, so it went
  onto the bartender's rail first and ran first.

- `after the cocktail -> $22`, `after the beer -> $10`,
  `after the lemonade -> $4` — the drinks' calls joined the bartender's
  rail as each drink was poured: lemonade, beer, cocktail.

- `still on the tab -> $22` — section A's wrong bill, line for line. You
  rebuilt the crew's bug inside the actor.

`cargo test` catches it (lines trimmed to the interesting ones):

```text
running 61 tests
test actor::tests::calls_reading_tickets_still_run_in_the_order_sent ... FAILED

failures:

---- actor::tests::calls_reading_tickets_still_run_in_the_order_sent stdout ----

thread 'actor::tests::calls_reading_tickets_still_run_in_the_order_sent' (321563613) panicked at src/actor.rs:286:9:
assertion `left == right` failed
  left: ["later", "second"]
 right: ["first", "second", "later"]

test result: FAILED. 60 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.06s
```

- `calls_reading_tickets_still_run_in_the_order_sent` — `later` ran
  first, then `second`. `first` is missing altogether: its call was still
  waiting off the rail when the test asked for the log.

The lesson: **an actor's order is the order calls join its mailbox.**
Anything that lets a call join later lets later calls overtake it. Ray's
actors make the same trade. To keep the order of one submitter's calls,
a call whose input is not ready holds up the calls behind it.

Change the code back and confirm `cargo test` is green again.

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
./scripts/compare.sh v1-day-05
```

**Inspecting instead of typing?** Jump to today's exact finished state,
poke at it, and come back:

```bash
git switch --detach v1-day-05
cargo test
git switch -          # back to where you were
```

**Either way**, today's complete change set — docs included — reads as one
diff:

```bash
git diff v1-day-04 v1-day-05
```

## 12. Recap

**What changed today:** you watched a lock keep every drink on a shared
tab and still get the bill wrong, because on a crew a quick slip
overtakes a slow one. An `Actor` gives state a home. One thread owns it,
calls wait in its mailbox, and they run one at a time, in the order they
were sent. Each call returns a ticket on the same pass as every task.
Actors never wait for each other, tasks read their answers like any
other, and a panicking call becomes an error while the actor carries on.
The actor keeps whatever the call changed first. `call_reading` lets a call
wait for a ticket without giving up its place in line, so the crew can
pour while the bartender writes. Thirteen new tests (61 in all) pin it
across three section checkpoints (`v1-day-05a`, `v1-day-05b`,
`v1-day-05c`), still with no dependencies.

**Deliberately naive:**

- every sender shares one mailbox, so a call waiting for its input holds
  up everyone's calls, where Ray holds up only the calls from the same
  submitter;

- a call is a Rust closure, so it can never leave this program;

- the state is built by the waiter and moved in, where Ray builds an
  actor's state inside the actor's own process;

- nobody can look an actor up by name or stop it early;

- and if an actor's thread itself ever died, every call still in its
  mailbox would report "worker stopped", and the tabs would be gone for
  good. That is Day 13's subject.

**That closes Part 1 — a mini-Ray on one machine.** Look at what the
kitchen can do now:

- **The cook and the ticket (Day 1):** a function becomes a task, with a
  ticket for its answer.

- **The pass (Day 2):** tickets can be copied, waited on, and passed into
  the next task before its input exists.

- **The pantry (Day 3):** every result sits once in one place, and any
  worker reads it without copying.

- **The crew (Day 4):** a crew of workers behind one rail runs a real job
  in a bit more than a quarter of the time.

- **The bar (Day 5):** actors keep state, and keep its order.

That is Ray's core programming model (tasks, objects, actors) in about
640 lines of engine code, comments included, with 61 tests and no
dependencies.

**This is where the free book ends.** Days 6 to 17, the distributed
half, are the Pro edition. The free book ends on something real: a working
mini-Ray you built by hand, every piece of it tested. Perhaps you would like
to carry it across machines: a cluster, objects that travel, ownership,
and recovery when workers, actors, and whole machines die. If so, the
repository's README is the place to find the Pro edition. Either way,
thank you for building this far.

**What the Pro edition does next (Day 6):** everything still lives inside
one program. A ticket points into this program's memory, a call is a
closure, a pot in the pantry is a Rust value. None of them can
leave the building. Day 6, *Speaking across the wire*, turns task calls
and results into bytes and back, whole. That is the border crossing every
distributed system lives at. From Day 7 the kitchen opens a second
restaurant across town, and the tickets, the pass and the bar you built
here carry over.

Tonight the kitchen closes with a crew, a bar, one pass, and one pantry
— see [the restaurant](story.md).
