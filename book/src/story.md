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

