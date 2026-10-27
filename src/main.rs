mod actor;
mod meter;
mod pool;
mod ticket;
mod worker;

use actor::Actor;
use pool::Pool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use ticket::{MemoryStore, Ticket, put, wait};
use worker::{Submit, Worker};

/// Today's big value: 64 MiB.
const STOCK_BYTES: usize = 64 * 1024 * 1024;

/// A big result: 64 MiB of bytes, the pot of stock that many orders need.
fn make_stock() -> Vec<u8> {
    vec![7; STOCK_BYTES]
}

/// Where a value's bytes sit in memory. Equal addresses mean the very same
/// bytes, not two copies.
fn address(bytes: &[u8]) -> usize {
    bytes.as_ptr() as usize
}

fn main() {
    one_cook_copies_or_shares();
    two_cooks_one_pass();
    the_waiter_stocks_the_pantry();
    picked_by_hand();
    one_rail_for_the_crew();
    the_crew_and_the_pantry();
    more_cooks_less_time();
    a_tab_on_the_crew();
    a_bartender_keeps_the_tab();
    two_bartenders_one_pass();
    the_crew_pours_the_bartender_writes();
}

/// One cook makes the stock; eight tasks need it.
fn one_cook_copies_or_shares() {
    let worker = Worker::start("worker-0");
    let stock = worker.submit(make_stock);
    let id = stock.id();
    let (made, _) = wait(vec![stock.clone()], 1, Duration::from_secs(60));
    println!(
        "submit make_stock         -> ticket {id}, ready: {}",
        made.len() == 1
    );

    let since = start_measuring();
    let copy = stock.get().expect("the stock never burns");
    let size = mib(copy.len());
    println!(
        "get {id}                    -> {size} MiB of bytes: {}",
        cost(since)
    );

    let since = start_measuring();
    let copies: Vec<Ticket<usize>> = (0..8)
        .map(|_| worker.submit_with(&stock, |bytes: Vec<u8>| address(&bytes)))
        .collect();
    let (copies, _) = wait(copies, 8, Duration::from_secs(60));
    let done = copies.len();
    println!("8 tasks, submit_with      -> {done} done: {}", cost(since));

    let since = start_measuring();
    let shared = stock.read().expect("the stock never burns");
    let size = mib(shared.len());
    println!(
        "read {id}                   -> {size} MiB of bytes: {}",
        cost(since)
    );

    let since = start_measuring();
    let readers: Vec<Ticket<usize>> = (0..8)
        .map(|_| worker.submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
        .collect();
    let (readers, _) = wait(readers, 8, Duration::from_secs(60));
    let done = readers.len();
    println!("8 tasks, submit_reading   -> {done} done: {}", cost(since));

    let home = address(&shared);
    let (copied, read) = (how_many_saw(&copies, home), how_many_saw(&readers, home));
    println!("saw {id}'s own bytes        -> copies: {copied} of 8, readers: {read} of 8");
}

/// How many of `tasks` reported the address `home`.
fn how_many_saw(tasks: &[Ticket<usize>], home: usize) -> usize {
    tasks
        .iter()
        .filter(|task| matches!(task.get(), Ok(seen) if seen == home))
        .count()
}

/// Two cooks share one pass: one numbering, and each reads the other's dishes.
fn two_cooks_one_pass() {
    let pass = MemoryStore::new();
    let cooks = [
        Worker::start_in("worker-1", &pass),
        Worker::start_in("worker-2", &pass),
    ];
    let quick = cooks[0].submit(|| 1_u64);
    println!("worker-1's quick dish     -> ticket {}", quick.id());
    let slow = cooks[1].submit(|| {
        thread::sleep(Duration::from_secs(1));
        2_u64
    });
    println!("worker-2's slow dish      -> ticket {}", slow.id());
    let clock = Instant::now();
    let (ready, _) = wait(vec![quick, slow], 2, Duration::from_secs(5));
    let after = round_to_100(clock.elapsed());
    println!(
        "wait for both, max 5 s    -> {} ready after about {after} ms",
        ready.len()
    );

    let stock = cooks[0].submit(make_stock);
    let id = stock.id();
    let home = address(&stock.read().expect("the stock never burns"));
    println!("worker-1 makes the stock  -> ticket {id}");
    let since = start_measuring();
    let reader = cooks[1].submit_reading(&stock, |bytes: &Vec<u8>| (cook_name(), address(bytes)));
    let (cook, seen) = reader.get().expect("reading never burns");
    let own = seen == home;
    println!(
        "{cook} reads {id}         -> {id}'s own bytes: {own}, {}",
        cost(since)
    );
}

/// The waiter brings a big value no cook made and puts it in the pantry once.
fn the_waiter_stocks_the_pantry() {
    let pass = MemoryStore::new();
    let cooks = [
        Worker::start_in("worker-1", &pass),
        Worker::start_in("worker-2", &pass),
    ];
    let busy: Vec<Ticket<()>> = cooks
        .iter()
        .map(|cook| cook.submit(|| thread::sleep(Duration::from_millis(300))))
        .collect();
    println!(
        "both cooks busy, 300 ms   -> tickets {} and {}",
        busy[0].id(),
        busy[1].id()
    );

    let delivery = make_stock();
    let home = address(&delivery);
    let since = start_measuring();
    let stock = put(&pass, delivery);
    let id = stock.id();
    let (ready, _) = wait(vec![stock.clone()], 1, Duration::ZERO);
    let ready = ready.len() == 1;
    println!(
        "put the delivery          -> ticket {id}, ready: {ready}, {}",
        cost(since)
    );
    let (done, _) = wait(busy, 2, Duration::ZERO);
    println!("cooks finished yet?       -> {} of 2", done.len());

    let since = start_measuring();
    let readers: Vec<Ticket<usize>> = (0..8)
        .map(|n| cooks[n % 2].submit_reading(&stock, |bytes: &Vec<u8>| address(bytes)))
        .collect();
    let (readers, _) = wait(readers, 8, Duration::from_secs(5));
    let done = readers.len();
    println!("8 tasks on 2 cooks        -> {done} done: {}", cost(since));
    let seen = how_many_saw(&readers, home);
    println!("saw the delivery's bytes  -> {seen} of 8");
}

/// Tonight's four slips: one big one (800 ms of cooking), then three quick
/// ones (200 ms each).
const SLIPS_MS: [u64; 4] = [800, 200, 200, 200];

/// Cook for `ms` milliseconds, then report who cooked and when, on `clock`,
/// the slip was done.
fn cook_for(ms: u64, clock: Instant) -> (String, Duration) {
    thread::sleep(Duration::from_millis(ms));
    (cook_name(), clock.elapsed())
}

/// Two cooks, each with a rail of their own; the waiter picks a cook for
/// every slip, in turn.
fn picked_by_hand() {
    let pass = MemoryStore::new();
    let cooks = [
        Worker::start_in("worker-1", &pass),
        Worker::start_in("worker-2", &pass),
    ];
    let clock = Instant::now();
    let slips: Vec<Ticket<(String, Duration)>> = SLIPS_MS
        .iter()
        .enumerate()
        .map(|(n, &ms)| cooks[n % 2].submit(move || cook_for(ms, clock)))
        .collect();
    report("picked by hand, 2 cooks", &slips);
}

/// When all four slips were done, then each cook's share of them: first the
/// cook who got the big slip, then the other one.
fn report(label: &str, slips: &[Ticket<(String, Duration)>]) {
    let done: Vec<(String, Duration)> = slips
        .iter()
        .map(|slip| slip.get().expect("no slip burns"))
        .collect();
    let whole = done.iter().map(|(_, at)| *at).max().expect("4 slips");
    println!(
        "{label:<26}-> all 4 slips done after about {} ms",
        round_to_100(whole)
    );
    // Split the slips in two: those the big slip's cook cooked, and the rest.
    let (big, other): (Vec<_>, Vec<_>) = done.iter().partition(|(cook, _)| *cook == done[0].0);
    for (who, theirs) in [("the big slip's cook", big), ("the other cook", other)] {
        let last = theirs.iter().map(|(_, at)| *at).max().unwrap_or_default();
        println!(
            "  {who:<24}-> {} of 4 slips, done at about {} ms, then idle {} ms",
            theirs.len(),
            round_to_100(last),
            round_to_100(whole - last)
        );
    }
}

/// The same four slips on a crew of two cooks sharing one rail.
fn one_rail_for_the_crew() {
    let pass = MemoryStore::new();
    let crew = Pool::start(2, &pass);
    let clock = Instant::now();
    let slips: Vec<Ticket<(String, Duration)>> = SLIPS_MS
        .iter()
        .map(|&ms| crew.submit(move || cook_for(ms, clock)))
        .collect();
    report("one rail, a crew of 2", &slips);
}

/// Held slips and the pantry, through a crew of four.
fn the_crew_and_the_pantry() {
    let pass = MemoryStore::new();
    let crew = Pool::start(4, &pass);
    let clock = Instant::now();
    let sauce = crew.submit(|| {
        thread::sleep(Duration::from_millis(300));
        "sauce".to_string()
    });
    let pasta = crew.submit_reading(&sauce, |sauce: &String| format!("pasta with {sauce}"));
    let quick: Vec<Ticket<String>> = (0..3).map(|_| crew.submit(cook_name)).collect();
    let (quick, _) = wait(quick, 3, Duration::from_secs(5));
    println!(
        "sauce cooking, pasta held -> {} quick slips done after about {} ms",
        quick.len(),
        round_to_100(clock.elapsed())
    );
    let dish = pasta.get().expect("the sauce never burns");
    println!(
        "{dish:<26}-> ready after about {} ms",
        round_to_100(clock.elapsed())
    );

    let stock = put(&pass, make_stock());
    let home = address(&stock.read().expect("a put value never burns"));
    let since = start_measuring();
    let readers: Vec<Ticket<(String, usize)>> = (0..8)
        .map(|_| {
            crew.submit_reading(&stock, |bytes: &Vec<u8>| {
                thread::sleep(Duration::from_millis(50));
                (cook_name(), address(bytes))
            })
        })
        .collect();
    let seen: Vec<(String, usize)> = readers
        .iter()
        .map(|reader| reader.get().expect("reading never burns"))
        .collect();
    println!("8 readers on a crew of 4  -> 8 done: {}", cost(since));
    let mut cooks: Vec<&String> = seen.iter().map(|(cook, _)| cook).collect();
    cooks.sort();
    cooks.dedup();
    let own = seen.iter().filter(|(_, at)| *at == home).count();
    println!(
        "cooks used, bytes seen    -> {} of 4 cooks, the stock's own bytes: {own} of 8",
        cooks.len()
    );
}

/// The slow job: count the primes among the numbers 1 to `PRIME_LIMIT`.
const PRIME_LIMIT: u64 = 6_000_000;

/// Whether `n` is prime, the slow way: try every divisor up to its square
/// root. Pure arithmetic, so it keeps one CPU core busy and nothing else.
fn is_prime(n: u64) -> bool {
    if n < 2 {
        return false;
    }
    let mut divisor = 2;
    while divisor * divisor <= n {
        if n.is_multiple_of(divisor) {
            return false;
        }
        divisor += 1;
    }
    true
}

/// How many of `numbers` are prime.
fn count_primes(numbers: &[u64]) -> usize {
    numbers.iter().filter(|n| is_prime(**n)).count()
}

/// Share `n` of `all`, when `all` is cut into `slips` runs of equal length.
fn share(all: &[u64], n: usize, slips: usize) -> &[u64] {
    let size = all.len().div_ceil(slips);
    let start = (n * size).min(all.len());
    let end = (start + size).min(all.len());
    &all[start..end]
}

/// Count the primes in `numbers` on `crew`, cut into `slips` slips that each
/// read their share in place; then add up the answers.
fn count_on(crew: &Pool, numbers: &Ticket<Vec<u64>>, slips: usize) -> usize {
    let tickets: Vec<Ticket<usize>> = (0..slips)
        .map(|n| {
            crew.submit_reading(numbers, move |all: &Vec<u64>| {
                count_primes(share(all, n, slips))
            })
        })
        .collect();
    tickets
        .iter()
        .map(|ticket| ticket.get().expect("counting never burns"))
        .sum()
}

/// Start a crew of `size` cooks on `pass`, run the job on it, and return
/// the answer and how long it took.
fn time_on(
    pass: &Arc<MemoryStore>,
    numbers: &Ticket<Vec<u64>>,
    size: usize,
    slips: usize,
) -> (usize, f64) {
    let crew = Pool::start(size, pass);
    let clock = Instant::now();
    let primes = count_on(&crew, numbers, slips);
    (primes, ms_since(clock))
}

/// Milliseconds passed since `clock` started, with the fraction.
fn ms_since(clock: Instant) -> f64 {
    clock.elapsed().as_secs_f64() * 1000.0
}

/// One slow job on crews of 1, 2 and 4 cooks, timed.
fn more_cooks_less_time() {
    let cores = thread::available_parallelism().map_or(1, |n| n.get());
    println!("this machine              -> {cores} cores");
    let pass = MemoryStore::new();
    let numbers = put(&pass, (1..=PRIME_LIMIT).collect::<Vec<u64>>());
    println!("the job                   -> primes up to {PRIME_LIMIT}, as 16 slips");
    let mut alone_ms = 0.0;
    for size in [1, 2, 4] {
        let (primes, ms) = time_on(&pass, &numbers, size, 16);
        if size == 1 {
            alone_ms = ms;
        }
        println!(
            "a crew of {size}               -> {primes} primes in {ms:.0} ms, {:.2}x as fast as 1 cook",
            alone_ms / ms
        );
    }

    let all = numbers.read().expect("a put value never burns");
    let clock = Instant::now();
    let first = count_primes(share(&all, 0, 16));
    let first_ms = ms_since(clock);
    let clock = Instant::now();
    let last = count_primes(share(&all, 15, 16));
    let last_ms = ms_since(clock);
    println!("slip 1 alone              -> {first} primes in {first_ms:.0} ms");
    println!("slip 16 alone             -> {last} primes in {last_ms:.0} ms");
    let (primes, ms) = time_on(&pass, &numbers, 4, 64);
    println!(
        "a crew of 4, 64 slips     -> {primes} primes in {ms:.0} ms, {:.2}x as fast as 1 cook",
        alone_ms / ms
    );
}

/// Table 4's evening: three drinks, each with its price in dollars and how
/// long it takes to pour, in milliseconds. The guests ask for the cocktail
/// first, the lemonade last.
const DRINKS: [(&str, u64, u64); 3] = [
    ("cocktail", 12, 300),
    ("beer", 6, 200),
    ("lemonade", 4, 100),
];

/// Analogy: the tabs — what each table owes so far, kept from one drink to
/// the next.
///
/// A running total in dollars for every table with an open tab.
#[derive(Default)]
struct Tabs {
    by_table: HashMap<u32, u64>,
}

impl Tabs {
    /// Put `price` on `table`'s tab, opening it if needed; return the new total.
    fn add(&mut self, table: u32, price: u64) -> u64 {
        let tab = self.by_table.entry(table).or_default();
        *tab += price;
        *tab
    }

    /// Close `table`'s tab: return what it owes (nothing, if no tab is open).
    fn close(&mut self, table: u32) -> u64 {
        self.by_table.remove(&table).unwrap_or(0)
    }

    /// What `table` owes so far, leaving its tab open.
    fn owed(&self, table: u32) -> u64 {
        self.by_table.get(&table).copied().unwrap_or(0)
    }
}

/// Wait for the evening's three drinks and the bill, then print the bill,
/// when it was all over, and the tab as each drink left it.
fn tell_the_evening(label: &str, steps: &[Ticket<u64>], clock: Instant) {
    let seen: Vec<u64> = steps
        .iter()
        .map(|step| step.get().expect("no drink spills"))
        .collect();
    println!(
        "{label:<26}-> the bill: ${}, after about {} ms",
        seen[3],
        round_to_100(clock.elapsed())
    );
    for ((drink, _, _), tab) in DRINKS.iter().zip(&seen) {
        println!("  after the {drink:<14}-> table 4's tab: ${tab}");
    }
}

/// Table 4's evening on a crew of four, the tabs behind one lock: each slip
/// pours its drink and writes it on the tab; the last slip asks for the bill.
fn a_tab_on_the_crew() {
    let crew = Pool::start(4, &MemoryStore::new());
    let tabs = Arc::new(Mutex::new(Tabs::default()));
    let clock = Instant::now();
    let mut slips: Vec<Ticket<u64>> = DRINKS
        .iter()
        .map(|&(_, price, ms)| {
            let tabs = Arc::clone(&tabs);
            crew.submit(move || {
                thread::sleep(Duration::from_millis(ms));
                tabs.lock().unwrap().add(4, price)
            })
        })
        .collect();
    let for_the_bill = Arc::clone(&tabs);
    slips.push(crew.submit(move || for_the_bill.lock().unwrap().close(4)));
    tell_the_evening("a crew, one locked tab", &slips, clock);
    let left = tabs.lock().unwrap().close(4);
    println!("  still on the tab        -> ${left}");
}

/// Table 4's evening with a bartender: an actor who owns the tabs and pours
/// every drink, one call at a time.
fn a_bartender_keeps_the_tab() {
    let bartender = Actor::start("bartender", &MemoryStore::new(), Tabs::default());
    let clock = Instant::now();
    let mut calls: Vec<Ticket<u64>> = DRINKS
        .iter()
        .map(|&(_, price, ms)| {
            bartender.call(move |tabs: &mut Tabs| {
                thread::sleep(Duration::from_millis(ms));
                tabs.add(4, price)
            })
        })
        .collect();
    calls.push(bartender.call(|tabs: &mut Tabs| tabs.close(4)));
    tell_the_evening("a bartender, one tab", &calls, clock);
    let left = bartender.call(|tabs: &mut Tabs| tabs.close(4));
    println!(
        "  still on the tab        -> ${}",
        left.get().expect("no drink spills")
    );
}

/// Two bartenders on one pass: each keeps tabs of their own, neither waits
/// for the other, a cook reads what one of them hands out, and a glass slips.
fn two_bartenders_one_pass() {
    let pass = MemoryStore::new();
    let crew = Pool::start(2, &pass);
    let bartenders = [
        Actor::start("bartender-1", &pass, Tabs::default()),
        Actor::start("bartender-2", &pass, Tabs::default()),
    ];
    let clock = Instant::now();
    let cocktail = bartenders[0].call(|tabs: &mut Tabs| {
        thread::sleep(Duration::from_millis(300));
        tabs.add(4, 12)
    });
    let beer = bartenders[1].call(|tabs: &mut Tabs| tabs.add(7, 6));
    for (who, table, ticket) in [
        ("bartender-2's beer", 7, &beer),
        ("bartender-1's cocktail", 4, &cocktail),
    ] {
        let tab = ticket.get().expect("no drink spills");
        println!(
            "{who:<26}-> ticket {}, table {table}'s tab: ${tab}, after about {} ms",
            ticket.id(),
            round_to_100(clock.elapsed())
        );
    }

    let bill = bartenders[0].call(|tabs: &mut Tabs| tabs.close(4));
    let each = crew.submit_reading(&bill, |bill: &u64| bill / 2);
    println!(
        "a cook splits bill {}     -> ticket {}, ${} each for 2 guests",
        bill.id(),
        each.id(),
        each.get().expect("splitting never burns")
    );

    let slipped = bartenders[1].call(|tabs: &mut Tabs| -> u64 {
        tabs.add(7, 6);
        panic!("the glass slipped")
    });
    let error = slipped.get().expect_err("the glass always slips");
    println!("another beer, table 7     -> {error}");
    let owed = bartenders[1].call(|tabs: &mut Tabs| tabs.owed(7));
    println!(
        "table 7's tab after that  -> ${}",
        owed.get().expect("reading a tab never burns")
    );
}

/// Table 4's evening once more: the crew pours the drinks side by side, and
/// the bartender writes each one on the tab, in the order the guests asked.
fn the_crew_pours_the_bartender_writes() {
    let pass = MemoryStore::new();
    let crew = Pool::start(4, &pass);
    let bartender = Actor::start("bartender", &pass, Tabs::default());
    let clock = Instant::now();
    let mut calls: Vec<Ticket<u64>> = DRINKS
        .iter()
        .map(|&(_, price, ms)| {
            let drink = crew.submit(move || {
                thread::sleep(Duration::from_millis(ms));
                price
            });
            bartender.call_reading(&drink, |tabs: &mut Tabs, price: &u64| tabs.add(4, *price))
        })
        .collect();
    calls.push(bartender.call(|tabs: &mut Tabs| tabs.close(4)));
    tell_the_evening("the crew pours, one tab", &calls, clock);
    let left = bartender.call(|tabs: &mut Tabs| tabs.close(4));
    println!(
        "  still on the tab        -> ${}",
        left.get().expect("no drink spills")
    );
}

/// The name of the thread running this code: inside a task, the cook's name.
fn cook_name() -> String {
    thread::current().name().unwrap_or("?").to_string()
}

/// Timings wobble by a millisecond or two; rounding keeps every run's output
/// identical.
fn round_to_100(elapsed: Duration) -> u128 {
    (elapsed.as_millis() + 50) / 100 * 100
}

/// Start measuring: the meter's count and the clock, both as of now.
fn start_measuring() -> (usize, Instant) {
    (meter::allocated(), Instant::now())
}

/// Time passed and fresh memory allocated since `start_measuring`.
fn cost(since: (usize, Instant)) -> String {
    let (bytes, clock) = since;
    let ms = clock.elapsed().as_secs_f64() * 1000.0;
    let fresh = meter::allocated() - bytes;
    format!("{ms:.1} ms, {:.1} MiB allocated", mib(fresh))
}

/// Bytes as mebibytes (1 MiB = 1024 × 1024 bytes).
fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[cfg(test)]
mod tests {
    use super::{count_on, count_primes, share};
    use crate::pool::Pool;
    use crate::ticket::{MemoryStore, put};

    #[test]
    fn count_primes_finds_the_primes_up_to_30() {
        let numbers: Vec<u64> = (1..=30).collect();
        // 2, 3, 5, 7, 11, 13, 17, 19, 23, 29
        assert_eq!(count_primes(&numbers), 10);
    }

    #[test]
    fn the_shares_cover_every_number_once() {
        let all: Vec<u64> = (1..=1000).collect();
        for slips in [1, 7, 16, 64] {
            let joined: Vec<u64> = (0..slips)
                .flat_map(|n| share(&all, n, slips).to_vec())
                .collect();
            assert_eq!(joined, all, "cut into {slips} slips");
        }
    }

    #[test]
    fn the_job_on_a_crew_gives_the_same_answer_as_one_cook() {
        let pass = MemoryStore::new();
        let crew = Pool::start(4, &pass);
        let all: Vec<u64> = (1..=10_000).collect();
        let expected = count_primes(&all);
        let numbers = put(&pass, all);
        assert_eq!(count_on(&crew, &numbers, 16), expected);
        assert_eq!(expected, 1229);
    }
}
