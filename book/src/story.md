# The restaurant chain: an analogy to help you build the right mental model

Every day of this book adds one scene to a single running story: **a
restaurant kitchen that grows into a chain.** Each piece of the engine you
build is one thing in that story — a cook at the stove, an order ticket, a
second restaurant across town. Keep the story in your head, and you can
always picture what the engine is doing, even on the days the code gets
dense.

This page keeps the whole story in one place:

- **Before you start**, read it through once. A few minutes here give you
  a map of where the course is going, and every later chapter will feel
  like a place you have already visited.

- **While you work**, come back whenever a name from an earlier day stops
  making sense. You rarely need to leave the chapter to do that: a concept
  name written like [this](#ticket) is a link to this page, and clicking it
  opens that concept's reminder right where you are reading. (Press Esc or
  click anywhere else to close it.)

- **The page grows with the book.** Every new day adds its scene here, so
  it always covers exactly the days you can read.

Each reminder has the same three parts: *what it is* in plain words, *in
the story* — the thing it is in the restaurant — and the day that
introduced it, if you want the full explanation again.

| Day | Scene | Concepts it adds |
|-----|-------|------------------|
| 1 | [One cook](#day-1--one-cook) | worker, task, ticket, get, task error |
| 2 | [The pass gets numbered spots](#day-2--the-pass-gets-numbered-spots) | ticket number, the pass, the cook's stub, wait, held slip, error propagation |
| 3 | [The pantry](#day-3--the-pantry) | the meter, the pantry, reading in place, one pass for the whole kitchen, put, sealed |
| 4 | [The crew](#day-4--the-crew) | the crew, one rail for the whole crew, anyone who takes slips, speedup, slip size |
| 5 | [The bar](#day-5--the-bar) | a lock keeps writes, not order; the bartender; the bartender's rail; a call; two bartenders; the slipped glass; a call that waits in line |

## Day 1 — one cook

The chain starts as one kitchen with **a single cook**. The waiter — your
program — never cooks: they pin an order slip to the rail, get a numbered
ticket back on the spot, and collect the dish later. When a dish burns,
the kitchen sends out a note instead of closing, and the cook moves on to
the next slip.

<div class="concept" id="worker">

**Worker — the cook** · [Day 1, section A](day-01-a-function-runs-elsewhere.md)

*What it is:* a separate thread that takes jobs from a queue and runs them one at a time, in the order they arrived. It keeps running until it is shut down, and it finishes its queue first.

*In the story:* the cook at the stove, working through order slips pinned to a rail. At closing time the cook finishes every slip already on the rail before going home.

</div>

<div class="concept" id="task">

**Task — a function sent to run elsewhere** · [Day 1, section B](day-01-a-function-runs-elsewhere.md)

*What it is:* a plain function handed to a worker to run. Submitting it returns immediately; the work happens on the worker. In Ray, this is calling `f.remote(…)`.

*In the story:* an order slip: the waiter writes it, pins it to the rail, and walks away.

</div>

<div class="concept" id="ticket">

**Ticket — a claim on an answer that doesn't exist yet** · [Day 1, section B](day-01-a-function-runs-elsewhere.md)

*What it is:* what submitting a task hands back at once: a reference to the task's future result. Ray calls it an `ObjectRef`.

*In the story:* the numbered order ticket the waiter gets the moment they pin a slip — long before the food exists. The ticket is not the food; it is a promise of food.

</div>

<div class="concept" id="get">

**get — waiting for the answer** · [Day 1, section B](day-01-a-function-runs-elsewhere.md)

*What it is:* blocks until the task has finished, then returns its result or its error. Ray's version is `ray.get(ref)`.

*In the story:* showing the ticket at the pass — the counter where finished dishes come out — and waiting there until that dish is ready.

</div>

<div class="concept" id="task-error">

**Task error — the burnt-dish note** · [Day 1, section C](day-01-a-function-runs-elsewhere.md)

*What it is:* when a task crashes, the crash is caught on the worker and delivered to the task's ticket as an error, and the worker carries on. Ray reports this as a `RayTaskError`.

*In the story:* a dish burns, so the kitchen sends a note to the pass instead of the dish — “sorry, the oven caught fire” — and the cook picks up the next slip.

</div>

## Day 2 — the pass gets numbered spots

Finished dishes stop travelling in private envelopes. They land on **the
pass** under their ticket numbers and stay there, so a ticket becomes just
a number: it can be photocopied, read twice, and handed to someone else.
A bell rings each time a dish lands. The waiter learns to glance at the
pass with a time limit and take whichever dishes are up. And an order that
needs another dish — pasta that needs the sauce — is clipped to the sauce's
empty spot, and goes to the cook only once the sauce lands. If the sauce
burned, the pasta is never cooked; its spot gets a copy of the note.

<div class="concept" id="ticket-number">

**Ticket number — an id for a result** · [Day 2, section A](day-02-tickets-you-can-pass-around.md)

*What it is:* a small, copyable id (`ObjectId`) naming one task's result, whether or not the result exists yet. A ticket is now that id plus a pointer to the store where the result will land, so copying a ticket copies the claim, never the result. Ray's `ObjectRef` is the same idea.

*In the story:* the number printed on the order ticket. Anyone holding a photocopy of the number can collect the same dish.

</div>

<div class="concept" id="the-pass">

**The pass — where results wait** · [Day 2, section A](day-02-tickets-you-can-pass-around.md)

*What it is:* a table shared by every thread (`MemoryStore`), from ticket number to the task's outcome — its value or its error. A lock makes threads take turns at it, and a condition variable wakes everyone waiting whenever a new outcome lands. Reading a result leaves it in place. Ray's version is the in-process memory store.

*In the story:* the counter where finished dishes come out, now with a numbered spot per ticket, and a bell the cook rings each time a dish lands.

</div>

<div class="concept" id="stub">

**The cook's stub — a guarantee that something will land** · [Day 2, section A](day-02-tickets-you-can-pass-around.md)

*What it is:* the duty to store one task's outcome (`Promise`). It is kept when the task finishes or fails; if the job is thrown away unrun, dropping the promise stores a "worker stopped" error instead, so no reader ever waits forever.

*In the story:* the stub the cook keeps when a slip goes on the rail. Whatever happens to the order, something ends up on the pass under that number.

</div>

<div class="concept" id="wait">

**wait — a glance at the pass, with a time limit** · [Day 2, section B](day-02-tickets-you-can-pass-around.md)

*What it is:* blocks until at least a given number of tickets have results — a failure counts — or a timeout passes, then splits the tickets into `(ready, pending)`, in their original order. Ray's version is `ray.wait`.

*In the story:* the waiter holding several tickets stands at the pass for at most a few seconds and leaves with whichever dishes are up.

</div>

<div class="concept" id="held-slip">

**Held slip — a task whose input is another ticket** · [Day 2, section C](day-02-tickets-you-can-pass-around.md)

*What it is:* `submit_with(&ticket, task)` returns a new ticket at once, but the task joins the worker's queue only when the input ticket's result lands; the task then receives the plain value. In Ray, passing an `ObjectRef` as a task argument does the same.

*In the story:* the pasta slip, clipped to the empty spot where the sauce will land. When the sauce lands, the cook unclips the slip and pins it to the rail.

</div>

<div class="concept" id="error-propagation">

**Error propagation — the note travels down the chain** · [Day 2, section C](day-02-tickets-you-can-pass-around.md)

*What it is:* if a held task's input failed, the task never runs, and its ticket carries the input's error, message intact — through any number of steps. Ray raises the original task's error at `ray.get` of any task downstream of it.

*In the story:* the sauce burned, so the pasta is never cooked: the burnt-dish note is copied onto the pasta's spot on the pass.

</div>

## Day 3 — the pantry

The kitchen discovers where its results really are. Every finished result
sits, as one pot, in exactly one place — **the pantry** — and its spot on
the pass holds only a label saying where. Collecting a dish the old way ladles a
full copy into a fresh pot; a meter on the supply line shows what that
costs when eight orders each need a huge pot of stock. So every task, and
the waiter too, learns to look into the pot where it stands. A second cook joins, and the pass
turns out to belong to the kitchen, not to a cook: one numbering, one
place to look. Last, the waiter carries a delivery straight into the
pantry — no slip, no cook — and everything in the pantry stays sealed, so
any number of cooks can read it at once.

<div class="concept" id="meter">

**The meter — counting fresh memory** · [Day 3, section A](day-03-the-object-store.md)

*What it is:* a global allocator that hands every memory request to the system's allocator and adds the request's size to a running total. A demo reads the total before and after a step, so copying shows up as a number. It is a measuring tool for the course, not part of Ray.

*In the story:* a meter on the kitchen's supply line, counting every new pot the kitchen fills.

</div>

<div class="concept" id="pantry">

**The pantry — where every value lives, once** · [Day 3, section A](day-03-the-object-store.md)

*What it is:* each stored value — a task's result or a put value — sits in exactly one shared allocation (an `Arc`); the store's table holds only a pointer to it. Readers get more pointers to the same value, never copies, and the value is freed when the last pointer is gone. Ray's object store is both halves: the pantry (where values live) and [the pass](#the-pass) (`MemoryStore`, the numbered table of labels pointing into it). Ray keeps one per node, in shared memory.

*In the story:* the pantry behind the pass. A spot on the pass holds a label saying where the pot stands; the pot itself is in the pantry.

</div>

<div class="concept" id="read-in-place">

**Reading in place — a label, not a ladle** · [Day 3, section A](day-03-the-object-store.md)

*What it is:* `Ticket::read` returns an `Arc<T>` pointing at the stored value, and `submit_reading` lends a task a read-only `&A` of its input; neither copies anything. `get` and `submit_with` remain, as "read, then copy." Ray's `ray.get` of a NumPy array is the same zero-copy read.

*In the story:* looking into the pot where it stands, instead of having a fresh pot ladled out for you.

</div>

<div class="concept" id="shared-pass">

**One pass for the whole kitchen — one store, many workers** · [Day 3, section B](day-03-the-object-store.md)

*What it is:* `Worker::start_in(name, &store)` starts a worker whose results land on a pass (`MemoryStore`) that other workers share. One pass means one numbering, one place for `wait` to look, and — through the pantry behind it — every worker can read every other worker's results in place. Ray's object store is both halves, the pass and the pantry, one per node; its object ids are unique across the cluster.

*In the story:* the pass belongs to the kitchen, not to a cook. Every cook puts dishes on it, and no two dishes ever share a number.

</div>

<div class="concept" id="put">

**put — the waiter stocks the pantry** · [Day 3, section C](day-03-the-object-store.md)

*What it is:* `put(&store, value)` stores a value the program already holds under a fresh id and returns its ticket, ready at once. No task and no worker are involved, and the value is moved in, not copied. Put values and task results share one store and one numbering. Ray's version is `ray.put`.

*In the story:* the waiter carries a delivery straight into the pantry — no order slip, no cook, no waiting in line — and gets a ticket for it on the spot.

</div>

<div class="concept" id="sealed">

**Sealed — nothing in the pantry changes** · [Day 3, section C](day-03-the-object-store.md)

*What it is:* a stored value is immutable: readers get only shared references (`&T`), so the compiler refuses any attempt to change it. That is what makes sharing safe — readers need no lock, nobody sees a half-changed value, and a copy can never go stale. A changed version is a new value with a new id. Ray's objects are immutable for the same reasons.

*In the story:* everything in the pantry is sealed. Cooks look; nobody pours anything back in.

</div>

## Day 4 — the crew

The kitchen hires a **crew**. At first the waiter picks a cook for every
slip, taking turns — and a quick slip waits behind a big slip on one
cook's rail while the cook at the next stove stands idle. So the cooks
give up their own rails: the waiter pins every slip to **one rail**, and
whichever cook is free walks over and takes the next one. Held slips and
the pantry work for the crew exactly as for a lone cook, because the
waiter hands both the same kinds of slips. Last, the kitchen times a
real job — counting primes — on crews of one, two, and four, and learns
that a crew is only as fast as its last slip: smaller slips keep every
cook busy to the end.

<div class="concept" id="crew">

**The crew — a worker pool** · [Day 4, section A](day-04-many-workers-one-scheduler.md)

*What it is:* a fixed number of worker threads (`Pool`), started once and reused for every task, all taking jobs from one shared queue and putting results on one store. Ray starts a pool of worker processes on every node — by default about one per CPU core — and reuses them for task after task.

*In the story:* a crew of cooks sharing one kitchen. No cook belongs to any slip; any free cook cooks the next one.

</div>

<div class="concept" id="shared-rail">

**One rail for the whole crew — the scheduler** · [Day 4, section A](day-04-many-workers-one-scheduler.md)

*What it is:* one channel whose receiving end is shared by every thread of the pool (`Arc<Mutex<Receiver<Job>>>`). A free thread locks it only long enough to take the next job, so whichever thread is free first gets the next job, and no job waits while a thread sits idle. That rule is today's scheduler. Ray's scheduler hands each waiting task to a free worker; ours lets free workers take tasks, which on one machine has the same effect.

*In the story:* the waiter pins every slip to one rail and never picks a cook. A cook who is free walks to the rail, takes the first slip, and steps away before cooking it.

</div>

<div class="concept" id="submit-trait">

**Anyone who takes slips — the `Submit` trait** · [Day 4, section B](day-04-many-workers-one-scheduler.md)

*What it is:* a trait whose default methods — `execute`, `submit`, `submit_reading`, `submit_with` — are written once, on top of two methods each kind of worker supplies: `queue` (where its jobs go) and `store` (where its results land). A lone `Worker` and a `Pool` both implement it, so every kind of task works on both. In Ray, `f.remote()` looks the same however many workers stand behind it.

*In the story:* the waiter hands a lone cook and a whole crew exactly the same slips — plain, held, or reading a pot in place.

</div>

<div class="concept" id="speedup">

**Speedup — more cooks, less time, measured** · [Day 4, section C](day-04-many-workers-one-scheduler.md)

*What it is:* the time a job takes on one worker divided by its time on N workers. It reaches N only when the whole job can be split into equal parts. Amdahl's law adds a cap: the part that cannot be split (handing out tasks, adding up answers) limits the speedup however many workers you add — on Day 4 that part takes well under a millisecond, so it starts to matter only once tasks cross a network. On Day 4's machine, a prime-counting job ran about 1.8× to 1.9× faster on two threads, and about 3.3× to 3.9× faster on four — never a clean 4×.

*In the story:* a crew of four does not cook a big order in exactly a quarter of the time: the waiter still hands out every slip, and the order is done only when the last slip is.

</div>

<div class="concept" id="slip-size">

**Slip size — how finely to cut a job** · [Day 4, section C](day-04-many-workers-one-scheduler.md)

*What it is:* how much of a job one task carries: split the job into more tasks, and each one is smaller. Too few, unequal tasks leave workers idle while the last big one finishes; too many tiny tasks spend their time on per-task overhead. Ray's tips for first-time users warn against tiny tasks for that reason.

*In the story:* cut a big order into a few large slips, and three cooks may wait for the fourth; cut it into crumbs, and the cooks spend the evening walking to the rail.

</div>

## Day 5 — the bar

A guest at the bar runs a **tab**, and a tab has to last from one drink
to the next — something no slip can do. The crew tries keeping one tab
book behind a lock: every drink gets written, but the bill, which takes
no time to pour, is written first and comes out empty. So the restaurant
hires a **bartender**: a worker with a memory — the tabs — and a rail of
their own. Nobody else touches the tabs; you pin a call to the
bartender's rail, get a ticket on the spot, and the bartender takes the
calls one at a time, in the order they were pinned. Two bartenders never
wait for each other, and cooks read their answers off the pass like any
dish. When a glass slips, a note comes back, the bartender stays on
shift, and whatever was already written on the tab stays written. Last,
the crew pours the drinks side by side while the bartender writes each
one down, in the order the guests asked — waiting at a call whose drink
isn't up yet, with every later call waiting behind it.

<div class="concept" id="one-lock">

**A lock keeps writes, not order** · [Day 5, section A](day-05-actors-workers-with-memory.md)

*What it is:* state shared by many tasks behind a lock (`Arc<Mutex<…>>`). The lock makes sure no two tasks change it at the same moment, so no change is lost; it does nothing about *which* task gets there first. On a pool, a quick task overtakes a slow one, so changes land in the order tasks finish, not the order they were sent. In Ray the tasks are separate processes and could not even share the lock; Ray's docs say to put such state in an actor instead.

*In the story:* one tab book for the whole crew. The cooks take turns writing in it, and every drink gets written — but the bill, which takes no time to pour, gets written first.

</div>

<div class="concept" id="actor">

**Actor — the bartender** · [Day 5, section A](day-05-actors-workers-with-memory.md)

*What it is:* a dedicated thread (`Actor<S>`) that owns a value of type `S` — the actor's state — and runs the calls sent to it, one at a time, on that thread. The state moves in when the actor starts and never comes out, so it needs no lock. Ray's actors are the same idea: `@ray.remote` on a class, one dedicated worker process per instance.

*In the story:* the bartender. Like a cook, the bartender works through what is pinned to their rail. Unlike a cook, they keep things behind the bar — today, the tabs. Nobody else reaches over the bar.

</div>

<div class="concept" id="mailbox">

**Mailbox — the bartender's rail** · [Day 5, section A](day-05-actors-workers-with-memory.md)

*What it is:* the actor's own channel of calls. The actor takes them strictly one at a time, oldest first, so the order calls were sent in is the order they run in — the actor's one promise. Ray keeps the same promise for the calls from one submitter.

*In the story:* the bartender's own rail. Calls are pinned to it in order, and the bartender never skips ahead.

</div>

<div class="concept" id="actor-call">

**A call — a slip for the bartender** · [Day 5, section A](day-05-actors-workers-with-memory.md)

*What it is:* `actor.call(|state| …)` sends a function that takes the actor's state (`&mut S`) and returns a ticket at once; the answer lands on the same pass as every task's result. It is our version of Ray's `actor.method.remote(…)`.

*In the story:* a slip pinned to the bartender's rail — "put a beer on table 4" — with a ticket for the answer, which comes out on the pass like any dish.

</div>

<div class="concept" id="two-actors">

**Many actors, one pass** · [Day 5, section B](day-05-actors-workers-with-memory.md)

*What it is:* every actor has its own thread, state, and mailbox, so actors never wait for each other. Their tickets share the store, and its numbering, with tasks and `put` values, so a task can read an actor's result and an actor can read a task's. In Ray, actor results are objects like any other.

*In the story:* two bartenders, each with their own tabs and rail. One pours a slow cocktail while the other serves a beer at once, and a cook splits a bill straight off the pass.

</div>

<div class="concept" id="actor-error">

**A failed call — the slipped glass** · [Day 5, section B](day-05-actors-workers-with-memory.md)

*What it is:* a call that panics becomes a task error on its own ticket, and the actor carries on with the next call. Nothing is rolled back: whatever the call changed before the panic stays changed, so calls should check first and change last. Ray behaves the same for an exception in an actor method; an actor whose process dies is Day 13's subject.

*In the story:* the bartender puts a beer on the tab, then drops the glass. A note goes out instead of the beer, the bartender stays on shift — and the beer stays on the tab.

</div>

<div class="concept" id="held-call">

**A call that waits in line** · [Day 5, section C](day-05-actors-workers-with-memory.md)

*What it is:* `actor.call_reading(&ticket, |state, value| …)` joins the mailbox at once. When its turn comes, the actor waits for the ticket's result to land, then reads it in place; every later call waits too, so the order holds. If the input failed, the call never runs and its ticket carries the same error. Unlike a held slip, it never steps aside — stepping aside would let later calls overtake it.

*In the story:* "when the cocktail is up, put it on table 4's tab." The bartender reaches that call and waits for the cocktail; the calls behind it wait too, so the drinks go on the tab in the order the guests asked.

</div>
