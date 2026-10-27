use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::ticket::MemoryStore;
use crate::worker::{Job, Submit};

/// Analogy: a crew of cooks working from one shared rail — whichever cook is
/// free takes the next slip.
///
/// A fixed number of worker threads behind one job queue. Every thread takes
/// the next job the moment it is free, so no job waits behind a busy thread
/// while another thread sits idle.
pub struct Pool {
    sender: Option<Sender<Job>>,
    handles: Vec<JoinHandle<()>>,
    store: Arc<MemoryStore>,
}

impl Pool {
    /// Start `size` worker threads, named `worker-1` to `worker-<size>`, that
    /// share one queue and put their results in `store`.
    pub fn start(size: usize, store: &Arc<MemoryStore>) -> Pool {
        let (sender, receiver) = mpsc::channel::<Job>();
        let rail = Arc::new(Mutex::new(receiver));
        let handles = (1..=size)
            .map(|n| {
                let rail = Arc::clone(&rail);
                thread::Builder::new()
                    .name(format!("worker-{n}"))
                    .spawn(move || take_slips(&rail))
                    .expect("failed to spawn worker thread")
            })
            .collect();
        Pool {
            sender: Some(sender),
            handles,
            store: Arc::clone(store),
        }
    }
}

impl Submit for Pool {
    fn queue(&self) -> &Sender<Job> {
        self.sender.as_ref().expect("pool is running")
    }

    fn store(&self) -> &Arc<MemoryStore> {
        &self.store
    }
}

/// Analogy: one cook's evening — take the next slip from the shared rail,
/// cook it, come back for another, until the rail is empty and closed.
///
/// The loop every pool thread runs.
fn take_slips(rail: &Mutex<Receiver<Job>>) {
    loop {
        // The lock is held only while taking a job, never while running it.
        let next = rail.lock().unwrap().recv();
        match next {
            Ok(job) => job(),
            // Every Sender is gone and the queue is empty: the evening is over.
            Err(_) => return,
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Closing the channel lets the workers finish the queue, then stop.
        drop(self.sender.take());
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Pool;
    use crate::ticket::{MemoryStore, Ticket, put, wait};
    use crate::worker::{Submit, Worker};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    /// The name of the thread running this code.
    fn cook_name() -> String {
        thread::current().name().unwrap_or("?").to_string()
    }

    #[test]
    fn a_crew_runs_every_slip_and_returns_its_result() {
        let crew = Pool::start(3, &MemoryStore::new());
        let squares: Vec<Ticket<u64>> = (1..=20_u64).map(|n| crew.submit(move || n * n)).collect();
        let total: u64 = squares.iter().map(|ticket| ticket.get().unwrap()).sum();
        assert_eq!(total, 2870);
    }

    #[test]
    fn a_free_cook_takes_the_next_slip_while_another_is_busy() {
        let crew = Pool::start(2, &MemoryStore::new());
        let (open_gate, gate) = mpsc::channel::<()>();
        let stuck = crew.submit(move || gate.recv().unwrap());
        let quick = crew.submit(|| 7_u64);
        // One cook is stuck behind the gate; only the other can cook `quick`.
        let (ready_now, _) = wait(vec![quick], 1, Duration::from_secs(10));
        open_gate.send(()).unwrap();
        assert_eq!(ready_now.len(), 1, "the free cook took the second slip");
        stuck.get().unwrap();
    }

    #[test]
    fn every_cook_in_the_crew_works_at_once() {
        let crew = Pool::start(3, &MemoryStore::new());
        let (tell, started) = mpsc::channel::<String>();
        let mut gates = Vec::new();
        let mut slips = Vec::new();
        for _ in 0..3 {
            let (open_gate, gate) = mpsc::channel::<()>();
            gates.push(open_gate);
            let tell = tell.clone();
            slips.push(crew.submit(move || {
                tell.send(cook_name()).unwrap();
                gate.recv().unwrap();
            }));
        }
        // All three slips must be cooking at the same time, each by a different cook.
        let mut names: Vec<String> = (0..3)
            .filter_map(|_| started.recv_timeout(Duration::from_secs(10)).ok())
            .collect();
        for open_gate in gates {
            let _ = open_gate.send(());
        }
        names.sort();
        assert_eq!(names, ["worker-1", "worker-2", "worker-3"]);
        for slip in slips {
            slip.get().unwrap();
        }
    }

    #[test]
    fn dropping_the_crew_finishes_every_queued_slip() {
        let done = Arc::new(AtomicUsize::new(0));
        let crew = Pool::start(2, &MemoryStore::new());
        for _ in 0..10 {
            let done = Arc::clone(&done);
            crew.execute(Box::new(move || {
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        drop(crew);
        assert_eq!(done.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn a_held_slip_takes_no_cook_while_it_waits() {
        let store = MemoryStore::new();
        let crew = Pool::start(1, &store);
        // An input nobody has cooked yet: its spot on the pass stays empty.
        let sauce_id = store.new_id();
        let sauce: Ticket<u64> = Ticket::new(sauce_id, Arc::clone(&store));
        let pasta = crew.submit_with(&sauce, |sauce| sauce + 1);
        // The crew's only cook is free: the held slip is clipped to the pass,
        // not on the rail.
        assert_eq!(crew.submit(|| 7_u64).get().unwrap(), 7);
        store.put(sauce_id, Ok(Arc::new(41_u64)));
        assert_eq!(pasta.get().unwrap(), 42);
    }

    /// Where a byte vector's contents sit in memory.
    fn address(bytes: &[u8]) -> usize {
        bytes.as_ptr() as usize
    }

    #[test]
    fn the_crew_reads_a_put_value_in_place() {
        let store = MemoryStore::new();
        let crew = Pool::start(3, &store);
        let stock = put(&store, vec![7_u8; 1000]);
        let home = address(&stock.read().unwrap());
        let readers: Vec<Ticket<usize>> = (0..6)
            .map(|_| crew.submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
            .collect();
        for reader in readers {
            assert_eq!(reader.get().unwrap(), home);
        }
    }

    #[test]
    fn a_failed_input_on_the_crew_passes_the_error_on() {
        let crew = Pool::start(2, &MemoryStore::new());
        let burnt = crew.submit(|| -> u64 { panic!("the oven caught fire") });
        let next = crew.submit_with(&burnt, |x| x + 1);
        assert_eq!(
            next.get().unwrap_err().to_string(),
            "task failed: the oven caught fire"
        );
        // The cook who burnt it is still working.
        assert_eq!(crew.submit(|| 3_u64).get().unwrap(), 3);
    }

    /// Written once, for anything that takes order slips.
    fn double_on(slip_taker: &impl Submit, n: u64) -> u64 {
        slip_taker.submit(move || n * 2).get().unwrap()
    }

    #[test]
    fn one_function_drives_a_lone_cook_or_a_crew() {
        let store = MemoryStore::new();
        let cook = Worker::start_in("worker-0", &store);
        let crew = Pool::start(2, &store);
        assert_eq!(double_on(&cook, 20), 40);
        assert_eq!(double_on(&crew, 21), 42);
    }
}
