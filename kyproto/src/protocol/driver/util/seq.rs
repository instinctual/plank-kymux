// Project Kyber: seq.rs
// Copyright © 2022-2026 Kyber SAS
// SPDX-License-Identifier: LicenseRef-Kyber-Commercial OR AGPL-3.0
//
// This file is both under dual license: AGPLv3 and a Commercial one.
//
// ----
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::marker::PhantomData;

/**
 * Some network protocols use a sequence number.
 *
 * If the app is running for long enough, the sequence number may wrap around:
 *  - 16-bit: ... -> 0xFFFE -> 0xFFFF -> 0x0000 -> 0x0001 -> ...
 *  - 32-bit: ... -> 0xFFFFFFFE -> 0xFFFFFFFF -> 0x00000000 -> 0x00000001 -> ...
 *
 * To handle this problem, there are basically two solutions:
 *  1. use a type wide-enough so that it never overflows (u64)
 *  2. accept that the sequence space is circular, and only make local
 *     comparisons by slitting the circular range in two halves around a current
 *     point (0xFFFF is before 0x4000, but is after 0x8000)
 *
 * (1) is simple, but if several sequence numbers are to be transmitted in a
 * datagram, a lot of content space may be wasted.
 *
 * (2) is better to minimize network overhead, but it adds complexity for
 * everything using the sequence number. In particular, since the sequence
 * space is circular, the local comparisons do not yield an order (not even a
 * partial order), because transitivity is not guaranteed (it is possible to
 * get a < b && b < c && c < a). Technically, this implies that any sequence
 * object could not implement the Ord trait, which prevents to use any function
 * requiring an Ord (e.g. Vec::binary_search()).
 *
 * The Sequencer provides a way to benefit from the advantages of both
 * approaches:
 *  - the sequence number transmitted on the wire can be small (16-bit or
 *    32-bit), and will wrap around on overflow (circular sequence space).
 *  - the Sequencer will "unroll" the circular sequence space to generate a
 *    64-bit number, yielding a total order to be used by the receiver side.
 *
 * Concretely, on each new sequence number received, the Sequencer determines
 * if it is in the past or in the future compared to the most recent sequence
 * number received. For example, a Sequencer<u16> will consider that 0x1234 is
 * before 0x5678 but after 0xFFFF.
 */
#[derive(Debug)]
pub(crate) struct Sequencer<T> {
    newest_seq: u64,
    _phantom: PhantomData<T>,
}

impl<T> Sequencer<T> {
    pub(crate) fn new() -> Self {
        Self {
            newest_seq: 0,
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_sequencer {
    ($($t:ty)*) => ($(
        #[allow(dead_code)]
        impl Sequencer<$t> {
            pub(crate) fn seq(&mut self, value: $t) -> u64 {
                const SIZE_BITS: usize = 8 * std::mem::size_of::<$t>();
                const HI_MASK: u64 = !((1 << SIZE_BITS) - 1);
                const RANGE: u64 = 1 << SIZE_BITS;
                const HALF_RANGE: u64 = 1 << (SIZE_BITS - 1);
                let mut seq = (self.newest_seq & HI_MASK) + value as u64;
                if self.newest_seq >= seq + HALF_RANGE {
                    seq += RANGE;
                } else if seq > self.newest_seq + HALF_RANGE && seq >= RANGE {
                    seq -= RANGE;
                }
                if seq > self.newest_seq {
                    self.newest_seq = seq;
                }
                seq
            }
        }
    )*)
}

impl_sequencer!(u8 u16 u32);

#[cfg(test)]
mod tests {
    use super::Sequencer;

    #[test]
    fn test_sequencer() {
        let mut sequencer = Sequencer::<u8>::new();

        assert_eq!(sequencer.seq(0x12), 0x12);
        assert_eq!(sequencer.seq(0x55), 0x55);
        assert_eq!(sequencer.seq(0x44), 0x44);
        assert_eq!(sequencer.seq(0x95), 0x95);
        assert_eq!(sequencer.seq(0x00), 0x100);
        assert_eq!(sequencer.seq(0xFF), 0xFF);
        assert_eq!(sequencer.seq(0x80), 0x180);
        assert_eq!(sequencer.seq(0x00), 0x200);
        assert_eq!(sequencer.seq(0x40), 0x240);
        assert_eq!(sequencer.seq(0x80), 0x280);
        assert_eq!(sequencer.seq(0x40), 0x240);
        assert_eq!(sequencer.seq(0xC0), 0x2C0);
        assert_eq!(sequencer.seq(0x40), 0x340);
        assert_eq!(sequencer.seq(0xC1), 0x2C1);
        assert_eq!(sequencer.seq(0xC0), 0x3C0);
    }

    #[test]
    fn test_sequencer_special() {
        let mut sequencer = Sequencer::<u8>::new();

        // When the provided value is distant by more than a half-range, the
        // sequencer considers it is a value from the past... except at the
        // beginning, because it may never give a value below 0
        assert_eq!(sequencer.seq(0xFF), 0xFF);

        assert_eq!(sequencer.seq(0x00), 0x100);
        assert_eq!(sequencer.seq(0xFF), 0xFF);
    }
}
