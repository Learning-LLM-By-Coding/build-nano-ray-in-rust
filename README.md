# Build nano-Ray in Rust

Build a distributed compute engine — a nano-[Ray](https://github.com/ray-project/ray)
— from an empty folder to a fault-tolerant cluster, in Rust, one tested
day at a time.

**Status: being written — Days 0–1 are done** (read them in
[`book/src`](book/src/introduction.md)). This book and
[Build nano-vLLM in Rust](https://github.com/Learning-LLM-By-Coding/build-nano-vllm-in-rust)
are two books of one series, written side by side. **Each book stands
alone** — nothing here will require owning the other
one. **Star or watch this repository** to catch the launch (and the
launch discount).

## Why Ray?

[Ray](https://github.com/ray-project/ray) is the open-source runtime that
much of modern ML infrastructure quietly runs on: training jobs spread over
hundreds of GPUs, batch inference over billions of rows, model serving that
scales up and down with traffic. When a large ML workload spans more than
one machine, there is a good chance a Ray-shaped runtime is doing the
spreading.

- **One machine eventually stops being enough — and then the questions
  change.** Where should this task run? Who owns that result, and when is
  it safe to delete? What happens when a worker, or a whole machine, dies
  halfway through a job? Those questions are the day-by-day syllabus below,
  and they are the same ones every distributed system answers, from Spark
  to Kubernetes controllers.

- **Ray's answers are small enough to build.** Its core is a handful of
  ideas — tasks, tickets for results that don't exist yet (`ObjectRef`s),
  a shared object store, actors, lineage-based recovery — that fit in one
  head once you have typed them yourself. That makes it the ideal teacher
  for distributed systems in general, not just for Ray.

- **Using a system teaches its flags; building one teaches its physics.**
  After you have written the scheduler, the ownership table, and the
  recovery path with your own hands, a stuck job, a spilled object, or a
  lost node stops being a mystery in a dashboard: you know which mechanism
  is involved and what it is waiting on.

## Why Rust?

Fair question — Ray itself is Python on the surface and C++ underneath.
Three honest reasons:

- **Concurrency is the subject, and Rust makes it checkable.** Threads,
  channels, shared state, messages crossing machines — this book is
  concurrency from Day 1. Rust refuses to compile a data race: a value
  shared between threads has to say *how* it is shared. So the compiler
  becomes a second teacher for exactly the lesson on the page, instead of
  a race showing up once in a thousand runs.

- **Ownership is literally a chapter.** Ray's hardest problem — when is a
  result no longer needed anywhere in the cluster? — is an ownership
  question. Rust makes you answer ownership questions for every value on
  one machine; Day 10 extends the same idea across machines, and it lands
  on ground you have been walking since Day 1.

- **Every checkpoint has to work forever.** The whole course rests on one
  promise: switch to any checkpoint tag, and the green bar passes. One
  pinned toolchain, no interpreter drift, few dependencies: Day 1 needs
  nothing beyond Rust's standard library. A checkpoint that compiled on tag day still compiles
  years later.

And since the language is taught as you go (see Prerequisites), you pick
up a working knowledge of Rust as a side effect — arguably a fourth reason.

## Why this course?

Knowing what each part of a distributed runtime does is not the same as
knowing how the parts behave *together* — and the second kind of
knowledge is the one you need when a job hangs, a node runs out of
memory, or a cluster needs to grow. There are three common ways to learn
Ray, and each one stops short of it:

- **Using Ray shows you the outside.** You learn `@ray.remote`,
  `ray.get`, and which settings are safe. But when a job stalls or objects
  start spilling to disk, the dashboard only shows the symptom. The cause
  lives in how the scheduler, the object store, and the ownership table
  affect each other — and hiding that is exactly what a good runtime is
  designed to do.

- **Tutorials teach one part at a time.** There are excellent
  explanations of tasks, of actors, of Ray's ownership paper. Each is
  right on its own, but the hard questions sit *between* the parts. Why
  does passing a ticket instead of its value change *where* the next task
  runs? Why can't a result be deleted while a ticket to it is still held
  on another machine? Why does losing one worker mean re-running tasks
  that already finished? No single-part tutorial can show you those
  answers, because each answer involves several parts at once.

- **Reading the real source is a second job.** Ray is hundreds of
  thousands of lines of C++ and Python, built for clusters of thousands
  of machines and a whole family of libraries. The core ideas are all in
  there, surrounded by everything production needs, and it is hard to
  tell the load-bearing code from the rest.

This course takes a fourth path: **build a nano version from scratch**,
keeping only the load-bearing parts. You write every one of them, one day
at a time, and each new part lands in a runtime that already works. So
you watch every new part change how the old ones behave. The object store
makes big results cheap to share — and then forces the question of when
they can be deleted. A second machine doubles your capacity — and then
makes *where* a task runs matter. Fault tolerance keeps answers arriving
— and then depends on every result remembering how it was made. By the
end you have not only built the parts; you have built the interactions
between them, which is what every real distributed system is made of.

The whole book runs on one story — a restaurant kitchen that grows into a
chain — and [The restaurant chain: an analogy to help you build the right mental model](book/src/story.md) maps
every concept onto it on a single page, one scene per finished day. The first six days will be free in this repository, and they build a
complete mini-Ray on one machine — enough to try that experience end to
end. The Pro edition carries the same runtime across machines: a real
cluster, distributed objects and ownership, and recovery from worker,
actor, and node failures.

## How it will work

The same executable-book format as the first course — **git history is
the curriculum**:

- one commit and one immutable checkpoint tag per section, so any day's
  exact state is one `git switch` away;

- a green bar (`cargo fmt`, `clippy`, every test) passing at every
  checkpoint;

- every transcript in the book reproduced from real runs, never typed
  from imagination.

## Prerequisites (planned)

Some programming experience, in any language — that is the whole list.
${\color{red}\textbf{Rust is not a prerequisite}}$: taught as you go, as
in the first book. ${\color{red}\textbf{Distributed systems are not a
prerequisite}}$: every idea arrives with a plain-English analogy first.

## The planned 18 days

Days 0–1 are built; the rest is planned, and day boundaries will be
refined as the book is written. Same shape as the first course, with a slightly longer paid
half: the subject is bigger. Days 0–5 free in this repository (a complete
mini-Ray on one machine), days 6–17 in the Pro edition (the distributed
half). And nano, not Ray: the core runtime only — no autoscaler, no
placement groups, none of the libraries (Serve, Data, Train, Tune).

| Day | Theme | In plain words | ≈&nbsp;Time |
|-----|-------|----------------|--------|
| 0 | Setup: toolchain and the empty tested scaffold | Install the tools and end with a tested, running (if empty) program. | 0.5&nbsp;h |
| 1 | A function runs elsewhere | Wrap a plain function as a task, hand it to a worker, and hold a ticket for the answer. | 1.5–&#8288;2&nbsp;h |
| 2 | Tickets you can pass around | Results live behind ids: get them, wait on them, and pass a ticket to the next task before the food is ready. | 1.5–&#8288;2&nbsp;h |
| 3 | The object store | One shared pantry per machine: put bytes in once, every worker reads them without copying. | 2&nbsp;h |
| 4 | Many workers, one scheduler | A pool of workers and a queue of tasks — watch a slow job drop to a quarter of its time, measured. | 2&nbsp;h |
| 5 | Actors: workers with memory | Some work needs state. An actor is a worker that remembers, with a mailbox of ordered messages. | 2&nbsp;h |
| 6 | Speaking across the wire | Task calls and results become bytes and come back whole — serialization, the border crossing every distributed system lives at. | 2&nbsp;h |
| 7 | Crossing the machine line | A worker on another machine runs your task over plain TCP — and everything built so far still works. | 2.5&nbsp;h |
| 8 | A cluster is born | A head node keeps the address book: nodes, actors, heartbeats; workers join and leave. | 2&nbsp;h |
| 9 | Objects across machines | The pantry goes distributed: find where a result lives and fetch it. | 2&nbsp;h |
| 10 | Who owns what | Ray's hardest question: when is an object safe to delete? Ownership and reference counting across machines. | 2.5&nbsp;h |
| 11 | Scheduling with taste | Place tasks where their inputs already live; push work away when a node is busy. | 2&nbsp;h |
| 12 | When a worker dies | Kill a worker mid-task and watch the answer still arrive: lineage re-execution. | 2&nbsp;h |
| 13 | When an actor dies | Restart policies, replayed mailboxes, and what "exactly once" really costs. | 2&nbsp;h |
| 14 | When a whole node dies | Lost objects get rebuilt from their history; the cluster shrinks and keeps serving. | 2.5&nbsp;h |
| 15 | Memory pressure | The pantry overflows: spill to disk, admit less, and survive the storm. | 2&nbsp;h |
| 16 | Resources and placement | Tasks declare what they need — CPU slots, GPU slots — and the scheduler honors the budget. | 2&nbsp;h |
| 17 | The payoff: serve LLMs on it | Run an inference engine as actors on your own cluster — a ready-made one, or the one you build in the nano-vLLM book — and close with one final benchmark. | 3&nbsp;h |

## The series

Each book stands alone: you can start here, and nothing in this book
will require the other. They meet only if you want them to — Day 17 can
run the inference engine you build in
[Build nano-vLLM in Rust](https://github.com/Learning-LLM-By-Coding/build-nano-vllm-in-rust)
on this book's cluster — and owning one book will earn a discount on the
next. New to both subjects? The vLLM book's **days 0–5 are free right
now** and make a natural first taste of the format.

## License

MIT for this preview repository — see [LICENSE](LICENSE).
