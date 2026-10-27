use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Analogy: a meter on the kitchen's supply line — it counts every byte of
/// fresh memory the program asks for, whichever thread asks.
///
/// A global allocator that hands every request to the system allocator and
/// adds its size to a running total.
struct Meter;

#[global_allocator]
static METER: Meter = Meter;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every request goes straight to the system allocator; we only count.
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Total bytes allocated since the program started. It never goes down:
/// freeing memory does not undo the work of having made it.
pub fn allocated() -> usize {
    ALLOCATED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::allocated;

    #[test]
    fn the_meter_counts_a_fresh_allocation() {
        let before = allocated();
        let block = vec![1_u8; 1 << 20];
        // Other tests allocate at the same time, so only a lower bound is certain.
        assert!(allocated() - before >= block.len());
    }
}
