# Introduction

You are going to build a distributed compute engine in Rust, from scratch,
in 18 days — the kind of runtime that takes ordinary functions, runs them on
whichever machine has room, keeps their results where the next step can
find them, and carries on when a worker, an actor, or a whole machine dies
halfway through.

By Day 17 your engine will have:

- **tasks** — any function, handed to a worker, with a ticket (an
  `ObjectRef`) for the answer you can wait on or pass along before it
  exists,

- **an object store** — one shared pantry per machine, so large results are
  written once and read without copying,

- **actors** — workers that keep state between calls, fed through an
  ordered mailbox,

- **a real cluster** — worker nodes on other machines over TCP, a head node
  that keeps the address book, and objects that move between machines on
  demand,

- **ownership and distributed reference counting** — the answer to Ray's
  hardest question: when is a result safe to delete?

- **fault tolerance** — lost results rebuilt from their lineage, restarted
  actors, and a cluster that survives losing a node,

- resource-aware scheduling, memory pressure handling, and a finale that
  serves an LLM inference engine as actors on your own cluster.

## Who this is for

You need some programming experience in any language. You do **not** need
to know Rust or distributed systems — each idea is introduced when the
engine first needs it, with a plain-English analogy before any code. The
first six days run on one laptop; the second half spreads the same engine
across processes and machines, and still runs on one laptop if that is all
you have.

## How the course works

Each day is one chapter and one theme, built as **two to four related
sections** — each section is one focused capability with its own runnable
finish, and a day's sections always tell a single story. Every section is
one git commit with its own checkpoint tag (`v1-day-03a`, `v1-day-03b`, …),
and the day's last section doubles as the day tag (`v1-day-03`). After
finishing a section, `./scripts/compare.sh <tag>` checks your code — and
only your code (`src/`, `tests/`, `Cargo.toml`; never chapters or README) —
against that checkpoint, ignoring comments, blank lines, and the order of
top-level items, so its ✓ means the section is done.

Each section introduces at most one new *systems* concept. The Rust
language itself is taught by reference instead of by detour: every code
section opens with a small **Rust used here** strip linking exactly the
concepts its code leans on (threads, channels, closures, …) to the matching
pages of Google's [Comprehensive Rust](https://google.github.io/comprehensive-rust/)
course — and those links open in a side panel *next to* the code, so you
can fill a gap without leaving the page (Esc closes the panel). The exact
upstream revision the course references is pinned as a git submodule
(`third_party/comprehensive-rust`), and one optional script stages the
linked pages locally for fully offline reading — see [Setup](setup.md). No
prior Rust is assumed; read only the strips you need.

Every chapter has the same shape: what you will have running, what its
output will show, why yesterday's engine is not enough, a mental model, the
code section by section, the exact commands to run, the output you should
see, tests, one deliberate way to break it, and a checkpoint.

## The 18 days

| Day | Theme | In plain words | ≈ Time |
|-----|-------|----------------|--------|
| 0 | Setup | Install the tools and end with a tested, running (if empty) program. | 0.5 h |
| 1 | A function runs elsewhere | Wrap a plain function as a task, hand it to a worker, and hold a ticket for the answer. | 1.5–2 h |
| 2 | Tickets you can pass around | Results live behind ids: get them, wait on them, and pass a ticket to the next task before the food is ready. | 1.5–2 h |
| 3 | The object store | One shared pantry per machine: put bytes in once, every worker reads them without copying. | 2 h |
| 4 | Many workers, one scheduler | A pool of workers and a queue of tasks — watch a slow job drop to a quarter of its time, measured. | 2 h |
| 5 | Actors: workers with memory | Some work needs state. An actor is a worker that remembers, with a mailbox of ordered messages. | 2 h |
| 6 | Speaking across the wire | Task calls and results become bytes and come back whole. | 2 h |
| 7 | Crossing the machine line | A worker on another machine runs your task over plain TCP. | 2.5 h |
| 8 | A cluster is born | A head node keeps the address book: nodes, actors, heartbeats. | 2 h |
| 9 | Objects across machines | The pantry goes distributed: find where a result lives and fetch it. | 2 h |
| 10 | Who owns what | Ownership and reference counting across machines. | 2.5 h |
| 11 | Scheduling with taste | Place tasks where their inputs live; push work away from busy nodes. | 2 h |
| 12 | When a worker dies | Lineage re-execution: the answer still arrives. | 2 h |
| 13 | When an actor dies | Restart policies, replayed mailboxes, and the real cost of "exactly once". | 2 h |
| 14 | When a whole node dies | Lost objects rebuilt from their history; the cluster keeps serving. | 2.5 h |
| 15 | Memory pressure | Spill to disk, admit less, survive the storm. | 2 h |
| 16 | Resources and placement | Tasks declare CPU and GPU slots; the scheduler honors the budget. | 2 h |
| 17 | The payoff: serve LLMs on it | An inference engine as actors on your own cluster, and one final benchmark. | 3 h |

Roughly 34 hours end to end.

Two rules keep the course honest:

1. **Every day ends green.** At every tag, formatting, lints, tests, the
   day's smoke command, and the book's own code examples all pass.

2. **Naive before clever.** Every clever mechanism arrives after a slow,
   obviously-correct version you already understand — so you can see
   exactly what the cleverness buys.

Head to [Setup](setup.md) to get your toolchain ready.
