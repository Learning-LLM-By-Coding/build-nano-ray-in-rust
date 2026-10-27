use std::any::Any;
use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};

use crate::ticket::{MemoryStore, ObjectId, Outcome, Ticket};

/// A unit of work: any function the worker may call exactly once, safe to
/// move to another thread.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// Analogy: one cook in the kitchen, working through order slips in order.
///
/// A background thread that runs jobs one at a time, in the order they
/// arrived.
pub struct Worker {
    sender: Option<Sender<Job>>,
    handle: Option<JoinHandle<()>>,
    store: Arc<MemoryStore>,
}

impl Worker {
    /// Start a named worker thread, waiting for jobs.
    pub fn start(name: &str) -> Worker {
        let (sender, receiver) = mpsc::channel::<Job>();
        let handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                // The loop ends once every Sender is gone and the queue is empty.
                for job in receiver {
                    job();
                }
            })
            .expect("failed to spawn worker thread");
        Worker {
            sender: Some(sender),
            handle: Some(handle),
            store: MemoryStore::new(),
        }
    }

    /// Queue `job` for the worker and return immediately. Fire and forget.
    pub fn execute(&self, job: Job) {
        // If the worker is gone the job is dropped unrun (its Promise reports it).
        let _ = self.sender.as_ref().expect("worker is running").send(job);
    }

    /// Hand `task` to the worker and return a ticket for its result at once.
    pub fn submit<T, F>(&self, task: F) -> Ticket<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + Sync + 'static,
    {
        let id = self.store.new_id();
        let promise = Promise::new(id, Arc::clone(&self.store));
        self.execute(Box::new(move || promise.keep(run_catching(task))));
        Ticket::new(id, Arc::clone(&self.store))
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Closing the channel lets the worker finish its queue, then stop.
        drop(self.sender.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Run `task`, turning a panic into a `TaskError` instead of a dead thread.
fn run_catching<T, F>(task: F) -> Outcome
where
    F: FnOnce() -> T,
    T: Send + Sync + 'static,
{
    match panic::catch_unwind(AssertUnwindSafe(task)) {
        Ok(value) => Ok(Arc::new(value)),
        Err(payload) => Err(TaskError::from_panic(payload)),
    }
}

/// Analogy: the cook's stub of the order slip — whatever happens to the
/// order, something lands on the pass under that number.
///
/// The duty to store one task's outcome. If the job is thrown away unrun,
/// `Drop` stores a "worker stopped" error, so no `get` waits forever.
struct Promise {
    id: ObjectId,
    store: Arc<MemoryStore>,
    kept: bool,
}

impl Promise {
    fn new(id: ObjectId, store: Arc<MemoryStore>) -> Promise {
        Promise {
            id,
            store,
            kept: false,
        }
    }

    /// Store the task's outcome under the promised id.
    fn keep(mut self, outcome: Outcome) {
        self.store.put(self.id, outcome);
        self.kept = true;
    }
}

impl Drop for Promise {
    fn drop(&mut self) {
        if !self.kept {
            let error = TaskError {
                message: "worker stopped before the task finished".to_string(),
            };
            self.store.put(self.id, Err(error));
        }
    }
}

/// Why a task produced no result.
#[derive(Clone, Debug)]
pub struct TaskError {
    message: String,
}

impl TaskError {
    /// Turn a panic's payload into a readable message. `panic!("literal")`
    /// carries a `&str`; `panic!("{x}")` with formatting carries a `String`.
    fn from_panic(payload: Box<dyn Any + Send>) -> TaskError {
        let message = if let Some(text) = payload.downcast_ref::<&str>() {
            text.to_string()
        } else if let Some(text) = payload.downcast_ref::<String>() {
            text.clone()
        } else {
            "task panicked".to_string()
        };
        TaskError { message }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task failed: {}", self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::{Promise, Worker};
    use crate::ticket::{MemoryStore, Ticket};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, mpsc};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn jobs_run_on_the_worker_thread_in_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let worker = Worker::start("worker-0");
        for n in 1..=3 {
            let seen = Arc::clone(&seen);
            worker.execute(Box::new(move || {
                let name = thread::current().name().unwrap_or("?").to_string();
                seen.lock().unwrap().push(format!("job {n} on {name}"));
            }));
        }
        drop(worker);
        let seen = seen.lock().unwrap();
        assert_eq!(
            *seen,
            [
                "job 1 on worker-0",
                "job 2 on worker-0",
                "job 3 on worker-0"
            ]
        );
    }

    #[test]
    fn dropping_the_worker_waits_for_queued_jobs() {
        let done = Arc::new(AtomicBool::new(false));
        let worker = Worker::start("worker-0");
        let flag = Arc::clone(&done);
        worker.execute(Box::new(move || {
            thread::sleep(Duration::from_millis(50));
            flag.store(true, Ordering::SeqCst);
        }));
        drop(worker);
        assert!(done.load(Ordering::SeqCst));
    }

    #[test]
    fn get_returns_the_task_result() {
        let worker = Worker::start("worker-0");
        assert_eq!(worker.submit(|| 6 * 7).get().unwrap(), 42);
        let greeting = worker.submit(|| format!("hello from {}", "worker-0"));
        assert_eq!(greeting.get().unwrap(), "hello from worker-0");
    }

    #[test]
    fn submit_returns_before_the_task_finishes() {
        let worker = Worker::start("worker-0");
        let (open_gate, gate) = mpsc::channel::<()>();
        // The task cannot finish until we open the gate...
        let ticket = worker.submit(move || {
            gate.recv().unwrap();
            "cooked"
        });
        // ...yet we already hold its ticket. Now let it finish.
        open_gate.send(()).unwrap();
        assert_eq!(ticket.get().unwrap(), "cooked");
    }

    #[test]
    fn a_panicking_task_becomes_an_error_on_its_ticket() {
        let worker = Worker::start("worker-0");
        let ticket = worker.submit(|| -> u64 { panic!("the oven caught fire") });
        let error = ticket.get().unwrap_err();
        assert_eq!(error.to_string(), "task failed: the oven caught fire");
    }

    #[test]
    fn formatted_panic_messages_survive_too() {
        let worker = Worker::start("worker-0");
        let table = 7;
        let ticket = worker.submit(move || -> u64 { panic!("table {table} sent it back") });
        assert_eq!(
            ticket.get().unwrap_err().to_string(),
            "task failed: table 7 sent it back"
        );
    }

    #[test]
    fn the_worker_survives_a_panicking_task() {
        let worker = Worker::start("worker-0");
        let bad = worker.submit(|| -> u64 { panic!("boom") });
        assert!(bad.get().is_err());
        assert_eq!(worker.submit(|| 3 * 3).get().unwrap(), 9);
    }

    #[test]
    fn a_burnt_dish_reads_the_same_through_every_copy() {
        let worker = Worker::start("worker-0");
        let burnt = worker.submit(|| -> u64 { panic!("the oven caught fire") });
        let copy = burnt.clone();
        let expected = "task failed: the oven caught fire";
        assert_eq!(burnt.get().unwrap_err().to_string(), expected);
        assert_eq!(copy.get().unwrap_err().to_string(), expected);
    }

    #[test]
    fn a_job_thrown_away_unrun_reports_worker_stopped() {
        let store = MemoryStore::new();
        let id = store.new_id();
        let ticket: Ticket<u64> = Ticket::new(id, Arc::clone(&store));
        drop(Promise::new(id, store));
        assert_eq!(
            ticket.get().unwrap_err().to_string(),
            "task failed: worker stopped before the task finished"
        );
    }
}
