use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

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

#[cfg(test)]
mod tests {
    use super::{MemoryStore, Outcome, Ticket};
    use std::sync::Arc;
    use std::thread;

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
}
