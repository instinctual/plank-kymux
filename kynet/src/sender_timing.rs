// SPDX-License-Identifier: AGPL-3.0-or-later
//! Opt-in, numeric-only diagnostic context. No logging, wire or pacing changes.
//! Scoped to a polled send future, not a worker thread: other streams cannot
//! contribute while this future is suspended. Disabled in ordinary builds.
use std::{cell::RefCell, future::Future, time::Duration};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Measurements {
    pub fec_total_ns: u64,
    pub fec_copy_ns: u64,
    pub fec_encoder_ns: u64,
    pub fec_repair_ns: u64,
    pub pacer_ns: u64,
    pub sleep_requested_ns: u64,
    pub sleeps: u64,
    pub quinn_ns: u64,
    pub quinn_max_ns: u64,
    pub datagrams: u64,
    pub datagram_bytes: u64,
}

tokio::task_local! {
    static CURRENT: RefCell<Measurements>;
}

pub fn ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

pub fn active() -> bool {
    CURRENT.try_with(|_| ()).is_ok()
}

pub fn update(f: impl FnOnce(&mut Measurements)) {
    let _ = CURRENT.try_with(|value| f(&mut value.borrow_mut()));
}

pub async fn measure<F: Future>(future: F) -> (F::Output, Measurements) {
    CURRENT
        .scope(RefCell::new(Measurements::default()), async {
            let output = future.await;
            (output, CURRENT.with(|value| *value.borrow()))
        })
        .await
}
