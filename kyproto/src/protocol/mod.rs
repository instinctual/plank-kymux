// Project Kyber: mod.rs
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

#![allow(unused)]

use crate::protocol::driver::{ProtocolRecvDriver, ProtocolSendDriver};
use crate::router::KyChannel;
use crate::{ProtocolError, ProtocolStats};

use kymux_types::*;

use async_trait::async_trait;
use kyutil::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub(crate) mod clock_sync;
pub(crate) mod driver;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum VideoProtocol {
    Reliable,
    GopStream,
    Unreliable,
    UnreliableFec,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum AudioProtocol {
    Reliable,
    Unreliable,
    UnreliableFec,
}

pub(crate) async fn start_video_protocol_send(
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
    protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
    let protocol = match video_protocol {
        VideoProtocol::Reliable => ProtocolSend::new(
            driver::av::reliable::ReliableProtocolSendDriver::start(ky_channel, protocol_stats)
                .await?,
        ),
        VideoProtocol::GopStream => ProtocolSend::new(
            driver::av::video_gopstream::VideoGopStreamProtocolSendDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
        VideoProtocol::Unreliable => ProtocolSend::new(
            driver::av::video_unreliable::VideoUnreliableProtocolSendDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
        VideoProtocol::UnreliableFec => ProtocolSend::new(
            driver::av::video_unreliable_fec::VideoUnreliableFecProtocolSendDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
    };

    Ok(protocol)
}

pub(crate) async fn start_video_protocol_recv(
    ky_channel: KyChannel,
    video_protocol: VideoProtocol,
    protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
    let protocol = match video_protocol {
        VideoProtocol::Reliable => ProtocolRecv::new(
            driver::av::reliable::ReliableProtocolRecvDriver::start(ky_channel, protocol_stats)
                .await?,
        ),
        VideoProtocol::GopStream => ProtocolRecv::new(
            driver::av::video_gopstream::VideoGopStreamProtocolRecvDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
        VideoProtocol::Unreliable => ProtocolRecv::new(
            driver::av::video_unreliable::VideoUnreliableProtocolRecvDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
        VideoProtocol::UnreliableFec => ProtocolRecv::new(
            driver::av::video_unreliable_fec::VideoUnreliableFecProtocolRecvDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
    };

    Ok(protocol)
}

pub(crate) async fn start_audio_protocol_send(
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
    protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
) -> Result<ProtocolSend<AVPacket>, ProtocolError> {
    let protocol = match audio_protocol {
        AudioProtocol::Reliable => ProtocolSend::new(
            driver::av::reliable::ReliableProtocolSendDriver::start(ky_channel, protocol_stats)
                .await?,
        ),
        AudioProtocol::Unreliable => ProtocolSend::new(
            driver::av::audio_unreliable::AudioUnreliableProtocolSendDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
        AudioProtocol::UnreliableFec => ProtocolSend::new(
            driver::av::audio_unreliable_fec::AudioUnreliableFecProtocolSendDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
    };

    Ok(protocol)
}

pub(crate) async fn start_audio_protocol_recv(
    ky_channel: KyChannel,
    audio_protocol: AudioProtocol,
    protocol_stats: &KyArc<KyMutex<ProtocolStats>>,
) -> Result<ProtocolRecv<AVPacket>, ProtocolError> {
    let protocol = match audio_protocol {
        AudioProtocol::Reliable => ProtocolRecv::new(
            driver::av::reliable::ReliableProtocolRecvDriver::start(ky_channel, protocol_stats)
                .await?,
        ),
        AudioProtocol::Unreliable => ProtocolRecv::new(
            driver::av::audio_unreliable::AudioUnreliableProtocolRecvDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
        AudioProtocol::UnreliableFec => ProtocolRecv::new(
            driver::av::audio_unreliable_fec::AudioUnreliableFecProtocolRecvDriver::start(
                ky_channel,
                protocol_stats,
            )
            .await?,
        ),
    };

    Ok(protocol)
}

pub(crate) async fn start_data_protocol(
    ky_channel: KyChannel,
) -> Result<(ProtocolSend<DataPacket>, ProtocolRecv<DataPacket>), ProtocolError> {
    let (ky_channel_recv, ky_channel_send) = ky_channel.into_split();

    let protocol_send = ProtocolSend::new(
        driver::data::reliable::ReliableProtocolSendDriver::start(ky_channel_send).await?,
    );
    let protocol_recv = ProtocolRecv::new(
        driver::data::reliable::ReliableProtocolRecvDriver::start(ky_channel_recv).await?,
    );

    Ok((protocol_send, protocol_recv))
}

pub(crate) async fn start_input_protocol(
    ky_channel: KyChannel,
) -> Result<(ProtocolSend<InputPacket>, ProtocolRecv<InputPacket>), ProtocolError> {
    let (ky_channel_recv, ky_channel_send) = ky_channel.into_split();

    let protocol_send = ProtocolSend::new(
        driver::input::reliable::ReliableProtocolSendDriver::start(ky_channel_send).await?,
    );
    let protocol_recv = ProtocolRecv::new(
        driver::input::reliable::ReliableProtocolRecvDriver::start(ky_channel_recv).await?,
    );

    Ok((protocol_send, protocol_recv))
}

pub(crate) async fn start_metrics_protocol_send(
    ky_channel: KyChannel,
) -> Result<ProtocolSend<MetricsPacket>, ProtocolError> {
    let protocol = ProtocolSend::new(
        driver::metrics::reliable::ReliableProtocolSendDriver::start(ky_channel).await?,
    );

    Ok(protocol)
}

pub(crate) async fn start_metrics_protocol_recv(
    ky_channel: KyChannel,
) -> Result<ProtocolRecv<MetricsPacket>, ProtocolError> {
    let protocol = ProtocolRecv::new(
        driver::metrics::reliable::ReliableProtocolRecvDriver::start(ky_channel).await?,
    );

    Ok(protocol)
}
