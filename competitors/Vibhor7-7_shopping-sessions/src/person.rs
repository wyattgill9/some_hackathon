//! Sessions, credit and assists, one partition of people at a time.
//!
//! A partition's events are never sorted. Two events less than 30 minutes apart
//! are in one session whatever lies between them, so events are dropped into
//! half-hour bins and only the bins are walked in order: a session ends where
//! the first event of one occupied bin is more than 30 minutes after the last
//! event of the one before. Getting each person's events into bin order is two
//! counting passes over the partition (by bin, then by person, the second one
//! stable), with no comparison whose outcome the CPU has to guess.
//!
//! Each person is then one pass over their events with two cursors into their
//! clicks: the clicks at or before the session start, and those more than 7
//! days before it.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::catalog::Catalog;
use crate::key::{self, Key};
use crate::parse::{Click, Event, Parsed, Pools, WidePurchases, CLICK_REACH_MS, DROPPED};
use crate::pool::Chunks;
use crate::sys;
use crate::ts::DAY_MS;

const SESSION_GAP_MS: u32 = 1_800_000;
const REACH: u32 = CLICK_REACH_MS as u32;
/// People with more clicks than this keep a running set of the campaigns in the 7-day window.
const SLIDING_MIN: usize = 16;
const ABSENT: u32 = u32::MAX;

/// One thread's user_daily rows, as columns.
#[derive(Default)]
pub struct UserRows {
    pub person: Vec<Key>,
    pub day: Vec<i32>,
    pub sessions: Vec<u32>,
    pub events: Vec<u32>,
    pub revenue: Vec<i64>,
    /// Category number + 1, or 0 for no purchases.
    pub top: Vec<u32>,
}

impl UserRows {
    fn with_capacity(rows: usize) -> Self {
        Self {
            person: Vec::with_capacity(rows),
            day: Vec::with_capacity(rows),
            sessions: Vec::with_capacity(rows),
            events: Vec::with_capacity(rows),
            revenue: Vec::with_capacity(rows),
            top: Vec::with_capacity(rows),
        }
    }
}

/// campaign_daily, indexed by `day * (campaigns + 1) + campaign`; `organic` is the last campaign.
pub struct Cells {
    /// sessions, purchasing_sessions, revenue_cents
    pub credited: Vec<[u64; 3]>,
    pub assisted: Vec<u32>,
}

pub struct Setup<'a> {
    pub catalog: &'a Catalog,
    pub pools: &'a Pools,
    pub parsed: &'a [Parsed],
    pub partitions: usize,
    /// Partition bits: the top bits of a person's hash, already used up.
    pub person_bits: u32,
    /// Timestamps are relative to this instant.
    pub epoch: i64,
    /// Epoch day of the window's first day.
    pub first_day: i64,
    pub days: usize,
    /// Half-hour bins from the timestamp epoch to the end of the window.
    pub bins: usize,
    pub wide: Option<&'a WidePurchases>,
}

/// Events and clicks of one partition, grouped by person.
#[derive(Default)]
struct Group {
    keys: Vec<Key>,
    values: Vec<u32>,
    mask: usize,
    shift: u32,
    bits: u32,
    people: Vec<Key>,
    event_cursor: Vec<u32>,
    event_start: Vec<u32>,
    click_cursor: Vec<u32>,
    click_start: Vec<u32>,
    event_person: Vec<u32>,
    click_person: Vec<u32>,
    /// Where each bin's events go in the staging arrays.
    bin_cursor: Vec<u32>,
    /// The partition's events in bin order: `ts << 32 | purchase`, and whose each is.
    staged: Vec<u64>,
    staged_person: Vec<u32>,
    /// `ts << 32 | purchase`, each person's together, in bin order.
    events: Vec<u64>,
    /// `[ts << 32 | campaign, click_id]`, each person's together.
    clicks: Vec<[u64; 2]>,
}

impl Group {
    fn reset(&mut self, expected_people: usize) {
        let capacity = (expected_people * 2).max(64).next_power_of_two();
        self.keys.clear();
        self.keys.resize(capacity, 0);
        self.values.resize(capacity, 0);
        self.mask = capacity - 1;
        self.shift = 64 - capacity.trailing_zeros();
        self.people.clear();
        self.event_cursor.clear();
    }

    #[inline(always)]
    fn home(&self, person: Key) -> usize {
        ((sys::mix(person) << self.bits) >> self.shift) as usize
    }

    /// The person's number in this partition, or ABSENT.
    #[inline(always)]
    fn find(&self, person: Key) -> u32 {
        let mut at = self.home(person);
        loop {
            let seen = unsafe { *self.keys.get_unchecked(at) };
            if seen == person {
                return unsafe { *self.values.get_unchecked(at) };
            }
            if seen == 0 {
                return ABSENT;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn find_or_add(&mut self, person: Key) -> u32 {
        let mut at = self.home(person);
        loop {
            let seen = unsafe { *self.keys.get_unchecked(at) };
            if seen == person {
                return unsafe { *self.values.get_unchecked(at) };
            }
            if seen == 0 {
                break;
            }
            at = (at + 1) & self.mask;
        }
        let number = self.people.len() as u32;
        self.keys[at] = person;
        self.values[at] = number;
        self.people.push(person);
        self.event_cursor.push(0);
        if self.people.len() * 2 > self.keys.len() {
            self.grow();
        }
        number
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let capacity = self.keys.len() * 4;
        self.keys.clear();
        self.keys.resize(capacity, 0);
        self.values.resize(capacity, 0);
        self.mask = capacity - 1;
        self.shift = 64 - capacity.trailing_zeros();
        for number in 0..self.people.len() {
            let person = self.people[number];
            let mut at = self.home(person);
            while self.keys[at] != 0 {
                at = (at + 1) & self.mask;
            }
            self.keys[at] = person;
            self.values[at] = number as u32;
        }
    }

    /// Turns per-person counts into start offsets (one extra at the end) and leaves the counts as write cursors.
    fn offsets(cursor: &mut [u32], start: &mut Vec<u32>) -> usize {
        start.clear();
        let mut total = 0u32;
        for count in cursor.iter_mut() {
            start.push(total);
            let n = *count;
            *count = total;
            total += n;
        }
        start.push(total);
        total as usize
    }

    fn load(&mut self, setup: &Setup, events: &[&Chunks], clicks: &[&Chunks], partition: usize) {
        let event_count: usize = events.iter().map(|c| c[partition].iter().map(|x| x.1 as usize).sum::<usize>()).sum();
        self.reset(event_count / 12);
        self.event_person.clear();
        self.event_person.reserve(event_count);
        self.bin_cursor.clear();
        self.bin_cursor.resize(setup.bins + 1, 0);
        // Pass 1: number the people, count events per person and per bin.
        let mut seen = 0;
        for chunks in events {
            for &(start, len) in &chunks[partition] {
                let records: &[Event] = unsafe { std::slice::from_raw_parts(setup.pools.events.at(start), len as usize) };
                for record in records {
                    let number = if record.ts == DROPPED {
                        ABSENT
                    } else {
                        let number = self.find_or_add(record.person);
                        unsafe {
                            *self.event_cursor.get_unchecked_mut(number as usize) += 1;
                            *self.bin_cursor.get_unchecked_mut((record.ts / SESSION_GAP_MS) as usize) += 1;
                        }
                        number
                    };
                    unsafe { *self.event_person.as_mut_ptr().add(seen) = number };
                    seen += 1;
                }
            }
        }
        unsafe { self.event_person.set_len(seen) };
        let live = Self::offsets(&mut self.event_cursor, &mut self.event_start);
        let mut total = 0u32;
        for count in self.bin_cursor.iter_mut() {
            let n = *count;
            *count = total;
            total += n;
        }
        // Every slot of these three is written below, exactly once: no need to zero them first.
        self.staged.clear();
        self.staged.reserve(live);
        self.staged_person.clear();
        self.staged_person.reserve(live);
        self.events.clear();
        self.events.reserve(live);
        unsafe {
            self.staged.set_len(live);
            self.staged_person.set_len(live);
            self.events.set_len(live);
        }
        // Pass 2: into bin order.
        let mut at = 0;
        for chunks in events {
            for &(start, len) in &chunks[partition] {
                let records: &[Event] = unsafe { std::slice::from_raw_parts(setup.pools.events.at(start), len as usize) };
                for record in records {
                    let number = unsafe { *self.event_person.get_unchecked(at) };
                    at += 1;
                    if number != ABSENT {
                        unsafe {
                            let cursor = self.bin_cursor.get_unchecked_mut((record.ts / SESSION_GAP_MS) as usize);
                            *self.staged.get_unchecked_mut(*cursor as usize) = (u64::from(record.ts) << 32) | u64::from(record.purchase);
                            *self.staged_person.get_unchecked_mut(*cursor as usize) = number;
                            *cursor += 1;
                        }
                    }
                }
            }
        }
        // Pass 3: to each person, keeping the bin order.
        for at in 0..live {
            unsafe {
                let cursor = self.event_cursor.get_unchecked_mut(*self.staged_person.get_unchecked(at) as usize);
                *self.events.get_unchecked_mut(*cursor as usize) = *self.staged.get_unchecked(at);
                *cursor += 1;
            }
        }

        // Clicks of people with no surviving event credit nothing: they are left out.
        self.click_cursor.clear();
        self.click_cursor.resize(self.people.len(), 0);
        self.click_person.clear();
        for chunks in clicks {
            for &(start, len) in &chunks[partition] {
                let records: &[Click] = unsafe { std::slice::from_raw_parts(setup.pools.clicks.at(start), len as usize) };
                for record in records {
                    let number = self.find(record.person);
                    if number != ABSENT {
                        unsafe { *self.click_cursor.get_unchecked_mut(number as usize) += 1 };
                    }
                    self.click_person.push(number);
                }
            }
        }
        let kept = Self::offsets(&mut self.click_cursor, &mut self.click_start);
        self.clicks.clear();
        self.clicks.reserve(kept);
        unsafe { self.clicks.set_len(kept) };
        let mut at = 0;
        for chunks in clicks {
            for &(start, len) in &chunks[partition] {
                let records: &[Click] = unsafe { std::slice::from_raw_parts(setup.pools.clicks.at(start), len as usize) };
                for record in records {
                    let number = unsafe { *self.click_person.get_unchecked(at) };
                    at += 1;
                    if number != ABSENT {
                        unsafe {
                            let cursor = self.click_cursor.get_unchecked_mut(number as usize);
                            *self.clicks.get_unchecked_mut(*cursor as usize) =
                                [(u64::from(record.ts) << 32) | u64::from(record.campaign), record.click];
                            *cursor += 1;
                        }
                    }
                }
            }
        }
    }
}

/// Everything a thread accumulates while walking people.
struct Tally {
    wide: Option<Vec<(u32, u32)>>,
    /// Milliseconds from the first day's midnight to the timestamp epoch (negative: the epoch is 7 days earlier).
    day_offset: i64,
    first_day: i64,
    width: usize,
    organic: usize,
    category_sum: Vec<i64>,
    category_touched: Vec<u32>,
    window_count: Vec<u32>,
    window_place: Vec<u32>,
    window: Vec<u32>,
    /// Purchases of the bin being read, until the day they count on is known.
    bin_purchases: Vec<u32>,
    rows: UserRows,
    cells: Cells,
    sessions: u64,
}

/// The clicks within reach of the session being closed: `clicks[oldest..upto]`.
struct Reach {
    oldest: usize,
    upto: usize,
    sliding: bool,
}

impl Tally {
    #[inline(always)]
    fn purchase(&self, packed: u32) -> (u32, u32) {
        match &self.wide {
            None => (packed & 0xff_ffff, packed >> 24),
            Some(table) => table[(packed - 1) as usize],
        }
    }

    #[inline(always)]
    fn window_add(&mut self, campaign: u32) {
        let count = &mut self.window_count[campaign as usize];
        if *count == 0 {
            self.window_place[campaign as usize] = self.window.len() as u32;
            self.window.push(campaign);
        }
        *count += 1;
    }

    #[inline(always)]
    fn window_remove(&mut self, campaign: u32) {
        let count = &mut self.window_count[campaign as usize];
        *count -= 1;
        if *count == 0 {
            let place = self.window_place[campaign as usize] as usize;
            let moved = self.window.pop().unwrap();
            if place < self.window.len() {
                self.window[place] = moved;
                self.window_place[moved as usize] = place as u32;
            }
        }
    }

    /// Ends a (person, day): writes its user_daily row and clears the category sums.
    #[inline(always)]
    fn row(&mut self, name: Key, day: u32, sessions: u32, events: u32, revenue: i64) {
        let (mut top, mut best) = (0u32, 0i64);
        for &category in &self.category_touched {
            let sum = std::mem::take(&mut self.category_sum[category as usize]);
            // Highest sum; on a tie the lower number, which is the byte-wise smaller name.
            if sum > best || (sum == best && category + 1 < top) {
                best = sum;
                top = category + 1;
            }
        }
        self.category_touched.clear();
        self.rows.person.push(name);
        self.rows.day.push((self.first_day + i64::from(day)) as i32);
        self.rows.sessions.push(sessions);
        self.rows.events.push(events);
        self.rows.revenue.push(revenue);
        self.rows.top.push(top);
    }

    /// Credits a finished session to a campaign and counts the campaigns that assisted it.
    #[inline(always)]
    fn credit(&mut self, clicks: &[[u64; 2]], reach: &mut Reach, start: u32, day: u32, revenue: i64, purchased: bool) {
        self.sessions += 1;
        while reach.upto < clicks.len() && (clicks[reach.upto][0] >> 32) as u32 <= start {
            if reach.sliding {
                self.window_add(clicks[reach.upto][0] as u32);
            }
            reach.upto += 1;
        }
        let earliest = start.saturating_sub(REACH);
        while reach.oldest < reach.upto && ((clicks[reach.oldest][0] >> 32) as u32) < earliest {
            if reach.sliding {
                self.window_remove(clicks[reach.oldest][0] as u32);
            }
            reach.oldest += 1;
        }
        let row = day as usize * self.width;
        let credited = if reach.upto > reach.oldest { clicks[reach.upto - 1][0] as u32 as usize } else { self.organic };
        let cell = &mut self.cells.credited[row + credited];
        cell[0] += 1;
        cell[1] += u64::from(purchased);
        cell[2] += revenue as u64;
        if reach.sliding {
            for &campaign in &self.window {
                self.cells.assisted[row + campaign as usize] += 1;
            }
        } else {
            for a in reach.oldest..reach.upto {
                let campaign = clicks[a][0] as u32;
                if !clicks[reach.oldest..a].iter().any(|c| c[0] as u32 == campaign) {
                    self.cells.assisted[row + campaign as usize] += 1;
                }
            }
        }
    }

    /// One person: `events` in bin order (any order within a bin), `clicks` in any order.
    fn person(&mut self, name: Key, events: &[u64], clicks: &mut [[u64; 2]]) {
        if clicks.len() > 1 {
            // By time; at one instant the smallest click_id last, so the credited click is always the last one at or before the start.
            clicks.sort_unstable_by(|a, b| (a[0] >> 32).cmp(&(b[0] >> 32)).then_with(|| key::compare(b[1], a[1])));
        }
        let mut reach = Reach { oldest: 0, upto: 0, sliding: clicks.len() > SLIDING_MIN };
        let count = events.len();
        let mut day = u32::MAX;
        let (mut day_sessions, mut day_events, mut day_revenue) = (0u32, 0u32, 0i64);
        // The session being built: it is closed when a later bin starts too long after `last`.
        let (mut start, mut last, mut size, mut revenue, mut purchased) = (0u32, 0u32, 0u32, 0i64, false);
        let mut open = false;
        let mut at = 0;
        while at < count {
            // One bin: its first and last instants, and its purchases.
            let first = at;
            let mut time = (events[at] >> 32) as u32;
            let bin = time / SESSION_GAP_MS;
            let (mut low, mut high) = (time, time);
            loop {
                let packed = events[at] as u32;
                if packed != 0 {
                    self.bin_purchases.push(packed);
                }
                at += 1;
                if at == count {
                    break;
                }
                time = (events[at] >> 32) as u32;
                if time / SESSION_GAP_MS != bin {
                    break;
                }
                low = low.min(time);
                high = high.max(time);
            }
            // Everything in a bin is within 30 minutes, so the only gap that can
            // end a session is the one from the last bin to this one.
            if open && low - last > SESSION_GAP_MS {
                day_sessions += 1;
                day_events += size;
                day_revenue += revenue;
                self.credit(clicks, &mut reach, start, day, revenue, purchased);
                open = false;
            }
            if !open {
                open = true;
                start = low;
                size = 0;
                revenue = 0;
                purchased = false;
                let this_day = ((i64::from(start) + self.day_offset) / DAY_MS) as u32;
                if this_day != day {
                    if day != u32::MAX {
                        self.row(name, day, day_sessions, day_events, day_revenue);
                    }
                    day = this_day;
                    day_sessions = 0;
                    day_events = 0;
                    day_revenue = 0;
                }
            }
            last = high;
            size += (at - first) as u32;
            // The bin's purchases count on the day its session started.
            for i in 0..self.bin_purchases.len() {
                let (price, category) = self.purchase(self.bin_purchases[i]);
                revenue += i64::from(price);
                purchased = true;
                let sum = &mut self.category_sum[category as usize];
                if *sum == 0 {
                    self.category_touched.push(category);
                }
                *sum += i64::from(price);
            }
            self.bin_purchases.clear();
        }
        day_sessions += 1;
        day_events += size;
        day_revenue += revenue;
        self.credit(clicks, &mut reach, start, day, revenue, purchased);
        self.row(name, day, day_sessions, day_events, day_revenue);
        if reach.sliding {
            for &campaign in &self.window {
                self.window_count[campaign as usize] = 0;
            }
            self.window.clear();
        }
    }
}

/// Walks every partition on `threads` threads. `finish` gets each thread's rows
/// (to write its user_daily file) while the thread is still running in parallel.
pub fn run<R: Send>(
    setup: &Setup,
    threads: usize,
    finish: impl Fn(usize, UserRows) -> R + Sync,
) -> (Vec<Cells>, Vec<R>, u64) {
    let events: Vec<&Chunks> = setup.parsed.iter().map(|p| &p.events).collect();
    let clicks: Vec<&Chunks> = setup.parsed.iter().map(|p| &p.clicks).collect();
    let campaigns = setup.catalog.campaign_ids.len();
    let width = campaigns + 1;
    let wide: Option<Vec<(u32, u32)>> = setup.wide.map(|table| table.lock().unwrap().clone());
    // A person is active on about one day for every nine events: enough room that the columns rarely grow.
    let event_records: usize = events.iter().map(|c| c.iter().flatten().map(|chunk| chunk.1 as usize).sum::<usize>()).sum();
    let expected_rows = event_records / 8 / threads + 64;
    let next = AtomicUsize::new(0);
    let results = sys::parallel(threads, |me| {
        let mut group = Group { bits: setup.person_bits, ..Group::default() };
        let mut tally = Tally {
            wide: wide.clone(),
            day_offset: setup.epoch - setup.first_day * DAY_MS,
            first_day: setup.first_day,
            width,
            organic: campaigns,
            category_sum: vec![0; setup.catalog.categories.len()],
            category_touched: Vec::new(),
            window_count: vec![0; campaigns],
            window_place: vec![0; campaigns],
            window: Vec::new(),
            bin_purchases: Vec::new(),
            rows: UserRows::with_capacity(expected_rows),
            cells: Cells { credited: vec![[0; 3]; width * setup.days], assisted: vec![0; width * setup.days] },
            sessions: 0,
        };
        loop {
            let partition = next.fetch_add(1, Ordering::Relaxed);
            if partition >= setup.partitions {
                break;
            }
            if events.iter().all(|c| c[partition].is_empty()) {
                continue;
            }
            group.load(setup, &events, &clicks, partition);
            for number in 0..group.people.len() {
                let (e0, e1) = (group.event_start[number] as usize, group.event_start[number + 1] as usize);
                let (c0, c1) = (group.click_start[number] as usize, group.click_start[number + 1] as usize);
                tally.person(group.people[number], &group.events[e0..e1], &mut group.clicks[c0..c1]);
            }
        }
        let sessions = tally.sessions;
        (tally.cells, finish(me, tally.rows), sessions)
    });
    let mut cells = Vec::new();
    let mut finished = Vec::new();
    let mut sessions = 0;
    for (c, f, s) in results {
        cells.push(c);
        finished.push(f);
        sessions += s;
    }
    (cells, finished, sessions)
}
