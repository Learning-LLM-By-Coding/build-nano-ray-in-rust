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

