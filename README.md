# Build nano-Ray in Rust

Build a distributed compute engine — a nano-[Ray](https://github.com/ray-project/ray)
— from an empty folder to a fault-tolerant cluster, in Rust, one tested
day at a time.

**Status: in design.** This is the next book in the series that begins
with [Build nano-vLLM in Rust](https://github.com/Learning-LLM-By-Coding/build-nano-vllm-in-rust);
writing starts when Volume 1 of that course completes. **Each book in
the series stands alone** — nothing here will require owning the other
one. **Star or watch this repository** to catch the launch (and the
launch discount).

## Why a distributed compute engine?

One machine eventually stops being enough — and the moment work spreads
across machines, a new set of questions takes over: where should this
task run, who owns that result, what happens when a worker (or a whole
node) dies mid-job? Ray is the open-source answer much of modern ML
infrastructure runs on, and its core ideas — tasks, actors, a shared
object store, lineage-based fault tolerance — fit in one head when you
build them yourself. Same conviction as the first book: using a system
teaches its flags; building one teaches its physics.

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

## The planned 16 days

Planned, not yet built — day boundaries will be refined as the book is
written. Same shape as the first course: days 0–5 free in this
repository (a complete mini-Ray on one machine), days 6–15 in the Pro
edition (the distributed half).

| Day | Theme | In plain words | ≈&nbsp;Time |
|-----|-------|----------------|--------|
| 0 | Setup: toolchain and the empty tested scaffold | Install the tools and end with a tested, running (if empty) program. | 0.5&nbsp;h |
| 1 | A function runs elsewhere | Wrap a plain function as a task, hand it to a worker, and hold a ticket for the answer. | 1.5–&#8288;2&nbsp;h |
| 2 | Tickets you can pass around | Results live behind ids: get them, wait on them, and pass a ticket to the next task before the food is ready. | 1.5–&#8288;2&nbsp;h |
| 3 | The object store | One shared pantry per machine: put bytes in once, every worker reads them without copying. | 2&nbsp;h |
| 4 | Many workers, one scheduler | A pool of workers and a queue of tasks — watch a slow job drop to a quarter of its time, measured. | 2&nbsp;h |
| 5 | Actors: workers with memory | Some work needs state. An actor is a worker that remembers, with a mailbox of ordered messages. | 2&nbsp;h |
| 6 | Crossing the machine line | Two processes on two machines talk over the network — and everything built so far still works. | 2.5&nbsp;h |
| 7 | A cluster is born | A head node keeps the address book: nodes, actors, heartbeats; workers join and leave. | 2&nbsp;h |
| 8 | Objects across machines | The pantry goes distributed: fetch what is remote, and track who owns what. | 2&nbsp;h |
| 9 | Scheduling with taste | Place tasks where their inputs already live; push work away when a node is busy. | 2&nbsp;h |
| 10 | Distributed garbage | Ray's hardest question: when is an object safe to delete? Ownership and reference counting across machines. | 2.5&nbsp;h |
| 11 | When a worker dies | Kill a worker mid-task and watch the answer still arrive: lineage re-execution. | 2&nbsp;h |
| 12 | When an actor dies | Restart policies, replayed mailboxes, and what "exactly once" really costs. | 2&nbsp;h |
| 13 | When a whole node dies | Lost objects get rebuilt from their history; the cluster shrinks and keeps serving. | 2.5&nbsp;h |
| 14 | Memory pressure | The pantry overflows: spill to disk, admit less, and survive the storm. | 2&nbsp;h |
| 15 | The payoff: serve LLMs on it | Run an inference engine as actors on your own cluster — a ready-made one, or the one you build in the nano-vLLM book — and close with one final benchmark. | 3&nbsp;h |

## The series

Each book stands alone: you can start here, and nothing in this book
will require the other. They meet only if you want them to — Day 15 can
run the inference engine you build in
[Build nano-vLLM in Rust](https://github.com/Learning-LLM-By-Coding/build-nano-vllm-in-rust)
on this book's cluster — and owning one book will earn a discount on the
next. New to both subjects? The vLLM book's **days 0–5 are free right
now** and make a natural first taste of the format.

## License

MIT for this preview repository — see [LICENSE](LICENSE).
