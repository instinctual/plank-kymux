// SPDX-License-Identifier: AGPL-3.0-or-later
//! Best-effort diagnostics, never I/O while holding Quinn's connection lock.

use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::mpsc::{self, SyncSender};
use std::thread::{self, JoinHandle};

use quinn_proto::ConnectionStats;

// One process-wide worker, not one thread per connection. At the usual 1 Hz
// sampling rate this bounds both retained memory and stale diagnostic backlog.
const QUEUE_CAPACITY: usize = 8;

pub(super) fn record(stats: ConnectionStats) {
    if stats.plank_telemetry.is_none() {
        return;
    }
    static SENDER: OnceLock<Option<SyncSender<ConnectionStats>>> = OnceLock::new();
    let sender = SENDER.get_or_init(|| {
        start_writer(io::stderr())
            .ok()
            .map(|(sender, _worker)| sender)
    });
    if let Some(sender) = sender {
        // A full queue, failed writer or unavailable thread loses diagnostics,
        // never media. Do not synchronously fall back to stderr or join this
        // process-lifetime worker during session/transport teardown.
        let _ = sender.try_send(stats);
    }
}

fn start_writer<W: Write + Send + 'static>(
    mut writer: W,
) -> io::Result<(SyncSender<ConnectionStats>, JoinHandle<()>)> {
    let (sender, receiver) = mpsc::sync_channel(QUEUE_CAPACITY);
    let worker = thread::Builder::new()
        .name("plank-quic-log".into())
        .spawn(move || {
            for stats in receiver {
                if write_snapshot(&mut writer, stats).is_err() {
                    break;
                }
            }
        })?;
    Ok((sender, worker))
}

fn write_snapshot(writer: &mut impl Write, stats: ConnectionStats) -> io::Result<()> {
    let Some(extra) = stats.plank_telemetry else {
        return Ok(());
    };
    let rtt_nanos = stats.path.rtt.as_nanos();
    let effective_pacing_bps = if rtt_nanos == 0 {
        0
    } else {
        let bits_per_second = u128::from(stats.path.cwnd)
            .saturating_mul(8)
            .saturating_mul(1_000_000_000)
            .saturating_mul(5)
            / rtt_nanos.saturating_mul(4);
        bits_per_second.min(u128::from(u64::MAX)) as u64
    };
    // Match the existing single-line format, including the separate queue and
    // MTU-drop counters. Neither this formatting nor the write runs in stats().
    let line = format!(
        "PLANK QUIC telemetry controller={} side={} remote={} rtt_us={} rtt_latest_us={} rtt_min_us={} rtt_max_us={} rtt_var_us={} cwnd_bytes={} in_flight_bytes={} in_flight_packets={} effective_pacing_bps={} controller_pacing_bps={} bandwidth_estimate_bps={} congestion_events={} sent_packets={} sent_bytes={} lost_packets={} lost_bytes={} mtu={} queue_limit_bytes={} queue_datagrams={} queue_payload_bytes={} queue_memory_bytes={} queue_high_water_payload_bytes={} queue_high_water_memory_bytes={} queue_evicted_datagrams={} queue_evicted_payload_bytes={} mtu_dropped_datagrams={} mtu_dropped_payload_bytes={}\n",
        extra.controller,
        extra.side,
        extra.remote,
        stats.path.rtt.as_micros(),
        extra.rtt_latest.as_micros(),
        extra.rtt_min.as_micros(),
        extra.rtt_max.as_micros(),
        extra.rtt_var.as_micros(),
        stats.path.cwnd,
        extra.in_flight_bytes,
        extra.in_flight_packets,
        effective_pacing_bps,
        extra.controller_pacing_bps,
        extra.bandwidth_estimate_bps,
        stats.path.congestion_events,
        stats.path.sent_packets,
        stats.udp_tx.bytes,
        stats.path.lost_packets,
        stats.path.lost_bytes,
        stats.path.current_mtu,
        extra.queue_limit_bytes,
        extra.queue_datagrams,
        extra.queue_payload_bytes,
        extra.queue_memory_bytes,
        extra.queue_high_water_payload_bytes,
        extra.queue_high_water_memory_bytes,
        extra.queue_evicted_datagrams,
        extra.queue_evicted_payload_bytes,
        extra.mtu_dropped_datagrams,
        extra.mtu_dropped_payload_bytes,
    );
    // Stderr locks for the whole write_all call. Do not issue individual
    // formatting-fragment writes that could interleave with other product logs.
    writer.write_all(line.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, Sender, TrySendError};
    use std::time::Duration;

    fn sample() -> ConnectionStats {
        let mut stats = ConnectionStats::default();
        stats.path.rtt = Duration::from_micros(20_000);
        stats.path.cwnd = 2_000_000;
        stats.path.congestion_events = 3;
        stats.path.sent_packets = 4;
        stats.udp_tx.bytes = 5;
        stats.path.lost_packets = 6;
        stats.path.lost_bytes = 7;
        stats.path.current_mtu = 1320;
        stats.plank_telemetry = Some(quinn_proto::PlankTelemetry {
            controller: "target-rate",
            side: "server",
            remote: "192.0.2.1:28989".parse().unwrap(),
            rtt_latest: Duration::from_micros(21_000),
            rtt_min: Duration::from_micros(19_000),
            rtt_max: Duration::from_micros(22_000),
            rtt_var: Duration::from_micros(1_000),
            in_flight_bytes: 8,
            in_flight_packets: 9,
            controller_pacing_bps: 10,
            bandwidth_estimate_bps: 11,
            queue_limit_bytes: 12,
            queue_datagrams: 13,
            queue_payload_bytes: 14,
            queue_memory_bytes: 15,
            queue_high_water_payload_bytes: 16,
            queue_high_water_memory_bytes: 17,
            queue_evicted_datagrams: 18,
            queue_evicted_payload_bytes: 19,
            mtu_dropped_datagrams: 20,
            mtu_dropped_payload_bytes: 21,
        });
        stats
    }

    #[test]
    fn preserves_existing_telemetry_fields_and_values() {
        let mut output = Vec::new();
        write_snapshot(&mut output, sample()).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            concat!(
                "PLANK QUIC telemetry controller=target-rate side=server remote=192.0.2.1:28989 ",
                "rtt_us=20000 rtt_latest_us=21000 rtt_min_us=19000 rtt_max_us=22000 rtt_var_us=1000 ",
                "cwnd_bytes=2000000 in_flight_bytes=8 in_flight_packets=9 effective_pacing_bps=1000000000 ",
                "controller_pacing_bps=10 bandwidth_estimate_bps=11 congestion_events=3 sent_packets=4 ",
                "sent_bytes=5 lost_packets=6 lost_bytes=7 mtu=1320 queue_limit_bytes=12 queue_datagrams=13 ",
                "queue_payload_bytes=14 queue_memory_bytes=15 queue_high_water_payload_bytes=16 ",
                "queue_high_water_memory_bytes=17 queue_evicted_datagrams=18 queue_evicted_payload_bytes=19 ",
                "mtu_dropped_datagrams=20 mtu_dropped_payload_bytes=21\n",
            )
        );
    }

    #[test]
    fn missing_snapshot_is_silent_and_pacing_calculation_is_bounded() {
        let mut output = Vec::new();
        record(ConnectionStats::default()); // does not start the logging worker
        write_snapshot(&mut output, ConnectionStats::default()).unwrap();
        assert!(output.is_empty());
        for (rtt, expected) in [(Duration::ZERO, 0), (Duration::from_nanos(1), u64::MAX)] {
            let mut stats = sample();
            stats.path.rtt = rtt;
            stats.path.cwnd = u64::MAX;
            output.clear();
            write_snapshot(&mut output, stats).unwrap();
            assert!(
                String::from_utf8(output.clone())
                    .unwrap()
                    .contains(&format!(" effective_pacing_bps={expected} "))
            );
        }
    }

    struct BlockedWriter {
        entered: Option<Sender<()>>,
        release: Receiver<()>,
    }

    impl Write for BlockedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn blocked_writer_does_not_block_producers_or_grow_the_queue() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (sender, worker) = start_writer(BlockedWriter {
            entered: Some(entered_tx),
            release: release_rx,
        })
        .unwrap();
        sender.try_send(sample()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        for _ in 0..QUEUE_CAPACITY {
            sender.try_send(sample()).unwrap();
        }
        for _ in 0..100 {
            assert!(matches!(
                sender.try_send(sample()),
                Err(TrySendError::Full(_))
            ));
        }
        // Teardown must be able to drop its sender without joining a stuck sink.
        drop(sender);
        release_tx.send(()).unwrap();
        worker.join().unwrap();
    }

    struct FailedWriter;
    impl Write for FailedWriter {
        fn write(&mut self, _bytes: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn failed_sink_disconnects_without_panicking_or_retrying() {
        let (sender, worker) = start_writer(FailedWriter).unwrap();
        sender.try_send(sample()).unwrap();
        worker.join().unwrap();
        assert!(matches!(
            sender.try_send(sample()),
            Err(TrySendError::Disconnected(_))
        ));
    }
}
