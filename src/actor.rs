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
