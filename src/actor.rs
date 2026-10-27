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

#[cfg(test)]
mod tests {
    use super::Actor;
    use crate::pool::Pool;
    use crate::ticket::{MemoryStore, Ticket, put, wait};
    use crate::worker::Submit;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

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
}
