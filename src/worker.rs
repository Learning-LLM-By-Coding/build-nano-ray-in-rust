use std::fmt;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

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
        }
    }

    /// Queue `job` for the worker and return immediately. Fire and forget.
    pub fn execute(&self, job: Job) {
        // A dead worker drops the job unrun; section B turns that into an error.
        let _ = self.sender.as_ref().expect("worker is running").send(job);
    }

    /// Hand `task` to the worker and return a ticket for its result at once.
    pub fn submit<T, F>(&self, task: F) -> Ticket<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (result_sender, result_receiver) = mpsc::channel();
        self.execute(Box::new(move || {
            let value = task();
            // The caller may have thrown the ticket away; nobody to tell.
            let _ = result_sender.send(value);
        }));
        Ticket {
            receiver: result_receiver,
        }
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

/// Analogy: the numbered order ticket — proof you are owed a dish that may
/// not be cooked yet.
///
/// The receiving end of a private one-result channel; `get` blocks until the
/// task's result arrives.
pub struct Ticket<T> {
    receiver: Receiver<T>,
}

impl<T> Ticket<T> {
    /// Wait until the task finishes, then hand over its result.
    pub fn get(self) -> Result<T, TaskError> {
        // If the job was dropped unrun, its sender is gone and recv fails.
        self.receiver.recv().map_err(|_| TaskError {
            message: "worker stopped before the task finished".to_string(),
        })
    }
}

/// Why a task produced no result.
#[derive(Debug)]
pub struct TaskError {
    message: String,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "task failed: {}", self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::Worker;
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
}
