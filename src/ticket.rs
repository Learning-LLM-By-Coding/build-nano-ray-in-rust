use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::worker::TaskError;

/// Analogy: the number printed on an order ticket.
///
/// A unique id naming one task's result, whether or not it exists yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ObjectId(u64);

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

/// What a finished task left behind: its value (type hidden), or why it failed.
pub type Outcome = Result<Arc<dyn Any + Send + Sync>, TaskError>;

/// Analogy: the pass — the counter where finished dishes wait under their
/// ticket numbers until someone collects them.
///
/// A shared table from object id to outcome. A condition variable wakes
/// every waiting thread whenever a new outcome lands.
pub struct MemoryStore {
    results: Mutex<HashMap<ObjectId, Outcome>>,
    landed: Condvar,
    next_id: AtomicU64,
}

impl MemoryStore {
    /// An empty pass, ready to be shared between threads.
    pub fn new() -> Arc<MemoryStore> {
        Arc::new(MemoryStore {
            results: Mutex::new(HashMap::new()),
            landed: Condvar::new(),
            next_id: AtomicU64::new(1),
        })
    }

    /// Hand out the next unused ticket number.
    pub fn new_id(&self) -> ObjectId {
        ObjectId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Record the outcome for `id` and wake everyone waiting.
    pub fn put(&self, id: ObjectId, outcome: Outcome) {
        self.results.lock().unwrap().insert(id, outcome);
        self.landed.notify_all();
    }

    /// Block until `id` has an outcome, then return a copy of it.
    pub fn get(&self, id: ObjectId) -> Outcome {
        let mut results = self.results.lock().unwrap();
        loop {
            if let Some(outcome) = results.get(&id) {
                return outcome.clone();
            }
            // Releases the lock while asleep; re-takes it before returning.
            results = self.landed.wait(results).unwrap();
        }
    }

    /// Block until at least `num_ready` of `ids` have outcomes, or until
    /// `timeout` runs out. Returns, for each id in order, whether it is ready.
    pub fn wait(&self, ids: &[ObjectId], num_ready: usize, timeout: Duration) -> Vec<bool> {
        let deadline = Instant::now() + timeout;
        let mut results = self.results.lock().unwrap();
        loop {
            let ready: Vec<bool> = ids.iter().map(|id| results.contains_key(id)).collect();
            let count = ready.iter().filter(|&&is_ready| is_ready).count();
            let now = Instant::now();
            if count >= num_ready || now >= deadline {
                return ready;
            }
            // Sleep until something lands or the time is up, whichever is first.
            results = self.landed.wait_timeout(results, deadline - now).unwrap().0;
        }
    }
}

/// Analogy: the numbered order ticket — and now that it is only a number,
/// you can photocopy it and hand copies around.
///
/// An object id plus the store it lives in. Cloning copies the claim, not
/// the result; every copy reads the same outcome.
#[derive(Clone)]
pub struct Ticket<T> {
    id: ObjectId,
    store: Arc<MemoryStore>,
    result_type: PhantomData<T>,
}

impl<T> Ticket<T> {
    /// A ticket for `id`, whose result will land in `store`.
    pub fn new(id: ObjectId, store: Arc<MemoryStore>) -> Ticket<T> {
        Ticket {
            id,
            store,
            result_type: PhantomData,
        }
    }

    /// The ticket's number.
    pub fn id(&self) -> ObjectId {
        self.id
    }
}

impl<T: Clone + 'static> Ticket<T> {
    /// Wait until the task finishes, then hand over a copy of its result.
    /// The result stays in the store, so any copy of the ticket can `get` it.
    pub fn get(&self) -> Result<T, TaskError> {
        let value = self.store.get(self.id)?;
        let value = value
            .downcast_ref::<T>()
            .expect("a ticket's type always matches its task's result");
        Ok(value.clone())
    }
}

/// Analogy: the waiter glances at the pass — for at most `timeout` — and
/// takes whichever ordered dishes are up, without waiting for all of them.
///
/// Like Ray's `ray.wait`: blocks until `num_ready` of `tickets` have results
/// (a failed task counts as ready) or `timeout` passes, then splits them into
/// `(ready, pending)`, each in the original order. All tickets must come from
/// the same worker.
pub fn wait<T>(
    tickets: Vec<Ticket<T>>,
    num_ready: usize,
    timeout: Duration,
) -> (Vec<Ticket<T>>, Vec<Ticket<T>>) {
    let Some(first) = tickets.first() else {
        return (Vec::new(), Vec::new());
    };
    let num_ready = num_ready.min(tickets.len());
    let ids: Vec<ObjectId> = tickets.iter().map(|ticket| ticket.id).collect();
    let landed = first.store.wait(&ids, num_ready, timeout);

    let mut ready = Vec::new();
    let mut pending = Vec::new();
    for (ticket, is_ready) in tickets.into_iter().zip(landed) {
        // Never hand back more than was asked for, even if more have landed.
        if is_ready && ready.len() < num_ready {
            ready.push(ticket);
        } else {
            pending.push(ticket);
        }
    }
    (ready, pending)
}

#[cfg(test)]
mod tests {
    use super::{MemoryStore, Outcome, Ticket, wait};
    use crate::worker::Worker;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    fn ready(value: u64) -> Outcome {
        Ok(Arc::new(value))
    }

    #[test]
    fn ids_are_unique_and_count_up_from_one() {
        let store = MemoryStore::new();
        let ids: Vec<String> = (0..3).map(|_| store.new_id().to_string()).collect();
        assert_eq!(ids, ["#1", "#2", "#3"]);
    }

    #[test]
    fn get_blocks_until_another_thread_puts_the_result() {
        let store = MemoryStore::new();
        let id = store.new_id();
        let ticket: Ticket<u64> = Ticket::new(id, Arc::clone(&store));
        let cook = thread::spawn(move || store.put(id, ready(42)));
        assert_eq!(ticket.get().unwrap(), 42);
        cook.join().unwrap();
    }

    #[test]
    fn every_copy_of_a_ticket_reads_the_same_result() {
        let store = MemoryStore::new();
        let id = store.new_id();
        let ticket: Ticket<u64> = Ticket::new(id, Arc::clone(&store));
        let copy = ticket.clone();
        store.put(id, ready(9));
        assert_eq!(ticket.get().unwrap(), 9);
        assert_eq!(ticket.get().unwrap(), 9);
        assert_eq!(copy.get().unwrap(), 9);
    }

    /// Three tickets on one store, none of them ready yet.
    fn three_orders(store: &Arc<MemoryStore>) -> Vec<Ticket<u64>> {
        (0..3)
            .map(|_| Ticket::new(store.new_id(), Arc::clone(store)))
            .collect()
    }

    fn numbers(tickets: &[Ticket<u64>]) -> Vec<String> {
        tickets
            .iter()
            .map(|ticket| ticket.id().to_string())
            .collect()
    }

    #[test]
    fn wait_returns_ready_tickets_in_order_and_at_most_num_ready() {
        let store = MemoryStore::new();
        let orders = three_orders(&store);
        store.put(orders[2].id(), ready(30));
        store.put(orders[0].id(), ready(10));

        let (ready_now, pending) = wait(orders.clone(), 1, Duration::ZERO);
        assert_eq!(numbers(&ready_now), ["#1"]);
        assert_eq!(numbers(&pending), ["#2", "#3"]);

        let (ready_now, pending) = wait(orders, 3, Duration::ZERO);
        assert_eq!(numbers(&ready_now), ["#1", "#3"]);
        assert_eq!(numbers(&pending), ["#2"]);
    }

    #[test]
    fn wait_gives_up_when_the_timeout_runs_out() {
        let store = MemoryStore::new();
        let orders = three_orders(&store);
        let clock = Instant::now();
        let (ready_now, pending) = wait(orders, 1, Duration::from_millis(50));
        assert!(ready_now.is_empty());
        assert_eq!(pending.len(), 3);
        assert!(clock.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn wait_wakes_as_soon_as_enough_results_land() {
        let store = MemoryStore::new();
        let orders = three_orders(&store);
        let id = orders[1].id();
        let cook = {
            let store = Arc::clone(&store);
            thread::spawn(move || store.put(id, ready(20)))
        };
        // A generous timeout: the test passes only if `put` wakes the wait.
        let (ready_now, _) = wait(orders, 1, Duration::from_secs(60));
        assert_eq!(numbers(&ready_now), ["#2"]);
        cook.join().unwrap();
    }

    #[test]
    fn a_failed_task_counts_as_ready() {
        let worker = Worker::start("worker-0");
        let burnt = worker.submit(|| -> u64 { panic!("burnt") });
        let (ready_now, _) = wait(vec![burnt], 1, Duration::from_secs(60));
        assert_eq!(ready_now.len(), 1);
        assert!(ready_now[0].get().is_err());
    }

    #[test]
    fn wait_on_no_tickets_returns_at_once() {
        let (ready_now, pending) = wait(Vec::<Ticket<u64>>::new(), 1, Duration::from_secs(60));
        assert!(ready_now.is_empty() && pending.is_empty());
    }
}
