// Project Kyber: webtransport_js.rs
// Copyright © 2022-2026 Kyber SAS
// SPDX-License-Identifier: LicenseRef-Kyber-Commercial OR AGPL-3.0-or-later
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

use crate::error::*;
use crate::{
    Connection, ConnectionDriver, ConnectionStats, RecvStream, RecvStreamDriver, SendStream,
    SendStreamDriver,
};

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use kymux_util::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::{spawn_local, JsFuture};

impl From<web_sys::WebTransport> for Connection {
    fn from(value: web_sys::WebTransport) -> Self {
        Self::new(WebTransportJSConnectionDriver::wrap(value))
    }
}

#[derive(Default)]
pub struct WebTransportJSOptions {
    pub congestion_control: WebTransportJSCongestionControl,
    pub require_unreliable: bool,
    pub server_certificate_hashes: Option<Vec<WebTransportJSHash>>,
}

impl WebTransportJSOptions {
    fn to_web_sys(&self) -> web_sys::WebTransportOptions {
        let options = web_sys::WebTransportOptions::new();
        options.set_require_unreliable(self.require_unreliable);
        options.set_congestion_control(self.congestion_control.to_web_sys());

        if let Some(server_certificate_hashes) = &self.server_certificate_hashes {
            let hashes = js_sys::Array::new();
            for hash in server_certificate_hashes {
                hashes.push(&hash.to_web_sys());
            }

            options.set_server_certificate_hashes(&hashes);
        }

        options
    }
}

#[derive(Default)]
pub enum WebTransportJSCongestionControl {
    #[default]
    Default,
    Throughput,
    LowLatency,
}

impl WebTransportJSCongestionControl {
    fn to_web_sys(&self) -> web_sys::WebTransportCongestionControl {
        match &self {
            Self::Default => web_sys::WebTransportCongestionControl::Default,
            Self::Throughput => web_sys::WebTransportCongestionControl::Throughput,
            Self::LowLatency => web_sys::WebTransportCongestionControl::LowLatency,
        }
    }
}

pub struct WebTransportJSHash {
    algorithm: String,
    value: Vec<u8>,
}

impl WebTransportJSHash {
    pub fn new(algorithm: impl Into<String>, value: Vec<u8>) -> Self {
        Self {
            algorithm: algorithm.into(),
            value,
        }
    }

    pub fn new_from_hex(algorithm: impl Into<String>, hash: &str) -> Result<Self, DecodeHexError> {
        let hash = hex_to_bytes(hash)?;
        let this = Self::new(algorithm, hash);
        Ok(this)
    }

    pub fn new_sha256_from_hex(hash: &str) -> Result<Self, DecodeHexError> {
        Self::new_from_hex("sha-256", hash)
    }

    fn to_web_sys(&self) -> web_sys::WebTransportHash {
        let array = js_sys::Uint8Array::from(self.value.as_ref());

        let obj = web_sys::WebTransportHash::new();
        obj.set_algorithm(&self.algorithm);
        obj.set_value(&array);
        obj
    }
}

#[derive(Debug)]
pub(crate) struct WebTransportJSConnectionDriver {
    web_transport: web_sys::WebTransport,
    uni_streams_reader: RefCell<Option<web_sys::ReadableStreamDefaultReader>>,
    bi_streams_reader: RefCell<Option<web_sys::ReadableStreamDefaultReader>>,
    datagrams_reader: RefCell<Option<web_sys::ReadableStreamDefaultReader>>,
    datagrams_writer: RefCell<Option<web_sys::WritableStreamDefaultWriter>>,
}

impl WebTransportJSConnectionDriver {
    fn wrap(web_transport: web_sys::WebTransport) -> Self {
        Self {
            web_transport,
            uni_streams_reader: RefCell::new(None),
            bi_streams_reader: RefCell::new(None),
            datagrams_reader: RefCell::new(None),
            datagrams_writer: RefCell::new(None),
        }
    }

    pub async fn connect(
        url: &str,
        options: &WebTransportJSOptions,
    ) -> Result<Connection, ConnectionError> {
        let options = options.to_web_sys();
        let web_transport = web_sys::WebTransport::new_with_options(url, &options)?;
        JsFuture::from(web_transport.ready()).await?;
        Ok(web_transport.into())
    }
}

#[async_trait(?Send)]
impl ConnectionDriver for WebTransportJSConnectionDriver {
    async fn open_uni(&self) -> Result<SendStream, ConnectionError> {
        let promise = self.web_transport.create_unidirectional_stream();
        let send_stream = JsFuture::from(promise)
            .await?
            .dyn_into::<web_sys::WritableStream>()?;
        let send_driver = WebTransportJSSendStreamDriver::new(send_stream);
        let send = SendStream::new(send_driver);
        Ok(send)
    }

    async fn open_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let promise = self.web_transport.create_bidirectional_stream();
        let bi_stream = JsFuture::from(promise)
            .await?
            .dyn_into::<web_sys::WebTransportBidirectionalStream>()?;
        let send_driver = WebTransportJSSendStreamDriver::new(bi_stream.writable().into());
        let recv_driver = WebTransportJSRecvStreamDriver::new(bi_stream.readable().into());
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn accept_uni(&self) -> Result<RecvStream, ConnectionError> {
        let promise = {
            let mut reader = self.uni_streams_reader.borrow_mut();
            if reader.is_none() {
                let uni_streams_reader = self
                    .web_transport
                    .incoming_unidirectional_streams()
                    .get_reader()
                    .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                    .map_err(|err| ConnectionError(format!("Unexpected reader type {err:?}")))?;
                *reader = Some(uni_streams_reader);
            }

            reader.as_ref().expect("Not initialized").read()
        };

        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Err(ConnectionError("No more unistreams".to_string()));
        }

        let recv_stream = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<web_sys::ReadableStream>()?;
        let recv_driver = WebTransportJSRecvStreamDriver::new(recv_stream);
        let recv = RecvStream::new(recv_driver);
        Ok(recv)
    }

    async fn accept_bi(&self) -> Result<(SendStream, RecvStream), ConnectionError> {
        let promise = {
            let mut reader = self.bi_streams_reader.borrow_mut();
            if reader.is_none() {
                let bi_streams_reader = self
                    .web_transport
                    .incoming_bidirectional_streams()
                    .get_reader()
                    .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                    .map_err(|err| ConnectionError(format!("Unexpected reader type {err:?}")))?;
                *reader = Some(bi_streams_reader);
            }

            reader.as_ref().expect("Not initialized").read()
        };

        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Err(ConnectionError("No more bistreams".to_string()));
        }

        let bi_stream = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<web_sys::WebTransportBidirectionalStream>()?;
        let send_driver = WebTransportJSSendStreamDriver::new(bi_stream.writable().into());
        let recv_driver = WebTransportJSRecvStreamDriver::new(bi_stream.readable().into());
        let send = SendStream::new(send_driver);
        let recv = RecvStream::new(recv_driver);
        Ok((send, recv))
    }

    async fn read_datagram(&self) -> Result<Bytes, ConnectionError> {
        let promise = {
            let mut reader = self.datagrams_reader.borrow_mut();
            if reader.is_none() {
                let datagrams_reader = self
                    .web_transport
                    .datagrams()
                    .readable()
                    .get_reader()
                    .dyn_into::<web_sys::ReadableStreamDefaultReader>()
                    .expect("Invalid readable stream");
                *reader = Some(datagrams_reader);
            }

            reader.as_ref().expect("Not initialized").read()
        };

        let obj = JsFuture::from(promise).await?;
        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Err(ConnectionError("Could not read datagrams".to_string()));
        }

        let vec = js_sys::Reflect::get(&obj, &JsValue::from("value"))?
            .dyn_into::<js_sys::Uint8Array>()?
            .to_vec();
        Ok(vec.into())
    }

    async fn send_datagram(&self, data: Bytes) -> Result<(), SendDatagramError> {
        let promise = {
            let mut writer = self.datagrams_writer.borrow_mut();
            if writer.is_none() {
                let datagrams_writer = self.web_transport.datagrams().writable().get_writer()?;
                *writer = Some(datagrams_writer);
            }

            let typed_array = js_sys::Uint8Array::from(&data[..]);
            let data_js_value = JsValue::from(typed_array);

            writer
                .as_ref()
                .expect("Not initialized")
                .write_with_chunk(&data_js_value)
        };

        JsFuture::from(promise).await?;

        Ok(())
    }

    async fn closed(&self) -> Result<(), ConnectionError> {
        let promise = self.web_transport.closed();
        let _ = JsFuture::from(promise).await?;
        Ok(())
    }

    fn close(&self, error_code: u32, reason: &str) {
        let close_info = web_sys::WebTransportCloseInfo::new();
        close_info.set_close_code(error_code);
        close_info.set_reason(reason);
        self.web_transport.close_with_close_info(&close_info);
    }

    fn max_datagram_size(&self) -> Option<usize> {
        let max = self.web_transport.datagrams().max_datagram_size();
        Some(max as usize)
    }

    async fn stats(&self) -> ConnectionStats {
        // Not implemented yet
        ConnectionStats {
            rtt: None,
            packets_lost: None,
        }
    }
}

#[derive(Debug)]
struct WebTransportJSSendStreamDriver {
    writer: web_sys::WritableStreamDefaultWriter,
    state: WriterState,
}

enum WriterState {
    Idle,
    Running(WriteFuture),
    Invalid,
}

impl std::fmt::Debug for WriterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterState::Idle => f.write_str("Idle"),
            WriterState::Running(_) => f.write_str("Running"),
            WriterState::Invalid => f.write_str("Invalid"),
        }
    }
}

impl WebTransportJSSendStreamDriver {
    fn new(send: web_sys::WritableStream) -> Self {
        let writer = send.get_writer().expect("Invalid writable stream");
        Self {
            writer,
            state: WriterState::Idle,
        }
    }
}

#[async_trait(?Send)]
impl SendStreamDriver for WebTransportJSSendStreamDriver {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        AsyncWriteExt::write(self, buf)
            .await
            .map_err(|e| WriteError(format!("{e}")))
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), WriteError> {
        AsyncWriteExt::write_all(self, buf)
            .await
            .map_err(|e| WriteError(format!("{e}")))
    }

    async fn finish(&mut self) -> Result<(), ClosedStreamError> {
        let promise = self.writer.close();
        JsFuture::from(promise).await?;
        Ok(())
    }

    fn reset(&mut self) {
        let promise = self.writer.abort();
        spawn_local(async move {
            let _ = JsFuture::from(promise).await;
        });
    }

    async fn closed(&mut self) -> Result<(), ConnectionError> {
        let promise = self.writer.closed();
        JsFuture::from(promise).await?;
        Ok(())
    }
}

impl AsyncWrite for WebTransportJSSendStreamDriver {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        loop {
            match std::mem::replace(&mut self.state, WriterState::Invalid) {
                WriterState::Idle => {
                    // Start writing
                    let fut = WriteFuture::start(&self.writer, buf);
                    self.state = WriterState::Running(fut);

                    // Report what was submitted to write
                    return Poll::Ready(Ok(buf.len()));
                }
                WriterState::Running(mut fut) => match Pin::new(&mut fut).poll(cx) {
                    Poll::Ready(res) => {
                        self.state = WriterState::Idle;
                        res?;
                    }
                    Poll::Pending => {
                        // Nope, we're occupied
                        self.state = WriterState::Running(fut);
                        return Poll::Pending;
                    }
                },
                WriterState::Invalid => unreachable!(),
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match std::mem::replace(&mut self.state, WriterState::Invalid) {
            WriterState::Idle => {
                self.state = WriterState::Idle;
                Poll::Ready(Ok(()))
            }
            WriterState::Running(mut fut) => match Pin::new(&mut fut).poll(cx) {
                Poll::Ready(res) => {
                    self.state = WriterState::Idle;
                    Poll::Ready(res)
                }
                Poll::Pending => {
                    self.state = WriterState::Running(fut);
                    Poll::Pending
                }
            },
            WriterState::Invalid => unreachable!(),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // There is no shutdown in the API, close() would close both directions,
        // so just flush
        self.poll_flush(cx)
    }
}

struct WriteFuture {
    fut: JsFuture,
}

impl WriteFuture {
    fn start(writer: &web_sys::WritableStreamDefaultWriter, buf: &[u8]) -> Self {
        // Start immediately to avoid buffer copy
        let typed_array = js_sys::Uint8Array::from(buf);
        let data_js_value = JsValue::from(typed_array);
        let promise = writer.write_with_chunk(&data_js_value);
        let fut = JsFuture::from(promise);
        Self { fut }
    }
}

impl Future for WriteFuture {
    type Output = std::io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = std::task::ready!(Pin::new(&mut self.fut).poll(cx))
            .map(|_| ())
            .map_err(js_value_to_io_error);
        Poll::Ready(res)
    }
}

#[derive(Debug)]
struct WebTransportJSRecvStreamDriver {
    reader: web_sys::ReadableStreamByobReader,
    state: ReaderState,
}

struct ReaderData {
    reader: web_sys::ReadableStreamByobReader,
    buf: BytesMut,
}

enum ReaderState {
    Idle(ReaderData),
    Running(ReadFuture),
    Invalid,
}

impl std::fmt::Debug for ReaderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReaderState::Idle(data) => f.debug_tuple("Idle").field(&data.buf.len()).finish(),
            ReaderState::Running(_) => f.write_str("Running"),
            ReaderState::Invalid => f.write_str("Invalid"),
        }
    }
}

impl WebTransportJSRecvStreamDriver {
    fn new(recv: web_sys::ReadableStream) -> Self {
        let options = web_sys::ReadableStreamGetReaderOptions::new();
        options.set_mode(web_sys::ReadableStreamReaderMode::Byob);
        let reader = recv
            .get_reader_with_options(&options)
            .dyn_into::<web_sys::ReadableStreamByobReader>()
            .expect("Invalid readable stream");
        let data = ReaderData {
            reader: reader.clone(),
            buf: BytesMut::new(),
        };
        Self {
            reader,
            state: ReaderState::Idle(data),
        }
    }
}

#[async_trait(?Send)]
impl RecvStreamDriver for WebTransportJSRecvStreamDriver {
    async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, ReadError> {
        if buf.is_empty() {
            return Ok(Some(0));
        }
        AsyncReadExt::read(self, buf)
            .await
            .map(|size| if size > 0 { Some(size) } else { None })
            .map_err(|e| ReadError(format!("{e}")))
    }

    fn stop(&mut self) {
        let promise = self.reader.cancel();
        spawn_local(async move {
            let _ = JsFuture::from(promise).await;
        });
    }

    async fn closed(&mut self) -> Result<(), ConnectionError> {
        let promise = self.reader.closed();
        JsFuture::from(promise).await?;
        Ok(())
    }
}

impl AsyncRead for WebTransportJSRecvStreamDriver {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            match std::mem::replace(&mut self.state, ReaderState::Invalid) {
                ReaderState::Idle(mut data) => {
                    if !data.buf.is_empty() {
                        // Data is available immediately
                        let n = data.buf.len().min(dst.remaining());
                        dst.put_slice(&data.buf[..n]);
                        data.buf.advance(n);
                        self.state = ReaderState::Idle(data);
                        return Poll::Ready(Ok(()));
                    }
                    self.state = ReaderState::Running(ReadFuture::new(data));
                }
                ReaderState::Running(mut fut) => match Pin::new(&mut fut).poll(cx) {
                    Poll::Ready(res) => {
                        // Report if the last operation failed or start again
                        self.state = ReaderState::Idle(fut.data);
                        res?;
                    }
                    Poll::Pending => {
                        // Nope, we're occupied
                        self.state = ReaderState::Running(fut);
                        return Poll::Pending;
                    }
                },
                ReaderState::Invalid => unreachable!(),
            }
        }
    }
}

struct ReadFuture {
    data: ReaderData,
    fut: Option<JsFuture>,
}

impl ReadFuture {
    fn new(data: ReaderData) -> Self {
        Self { data, fut: None }
    }

    fn poll_internal(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<usize>> {
        let this = Pin::into_inner(self);

        let fut = this.fut.get_or_insert_with(|| {
            this.data.buf.resize(8192, 0);
            let typed_array = js_sys::Uint8Array::new(&JsValue::from(this.data.buf.len()));
            let promise = this.data.reader.read_with_array_buffer_view(&typed_array);
            JsFuture::from(promise)
        });

        let obj = std::task::ready!(Pin::new(fut).poll(cx)).map_err(js_value_to_io_error)?;
        this.fut = None;

        let done = js_sys::Reflect::get(&obj, &JsValue::from("done"))
            .map_err(js_value_to_io_error)?
            .as_bool()
            .unwrap_or(false);
        if done {
            return Poll::Ready(Ok(0));
        }

        let array = js_sys::Reflect::get(&obj, &JsValue::from("value"))
            .map_err(js_value_to_io_error)?
            .dyn_into::<js_sys::Uint8Array>()
            .expect("Unexpected read value");

        let len = array.byte_length() as usize;
        // The array is initially created with a length of data.buf.len()
        array.copy_to(&mut this.data.buf[..len]);

        Poll::Ready(Ok(len))
    }
}

impl Future for ReadFuture {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = std::task::ready!(self.as_mut().poll_internal(cx));
        match res {
            Ok(n) => self.data.buf.truncate(n),
            Err(_) => self.data.buf.clear(),
        }
        Poll::Ready(res)
    }
}

impl From<JsValue> for ConnectionError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for ReadError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for ReadExactError {
    fn from(value: JsValue) -> Self {
        Self::ReadError(value.into())
    }
}

impl From<JsValue> for WriteError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for SendDatagramError {
    fn from(value: JsValue) -> Self {
        Self(format!("{value:?}"))
    }
}

impl From<JsValue> for ClosedStreamError {
    fn from(_: JsValue) -> Self {
        Self
    }
}

// For convenience, also provide the conversion from these errors to JsValue
// (containing the error message). They could not yield the original JsValue,
// which is not stored in the kyproto errors).
//
// This implementation could not be done by the client, since neither JsValue
// nor kyproto errors are defined in the client crate.

macro_rules! impl_from_jsvalue {
    ($t:ty) => {
        impl From<$t> for wasm_bindgen::JsValue {
            fn from(value: $t) -> Self {
                Self::from(&value.to_string())
            }
        }
    };
}

impl_from_jsvalue!(ConnectionError);
impl_from_jsvalue!(SendDatagramError);
impl_from_jsvalue!(ReadError);
impl_from_jsvalue!(ReadExactError);
impl_from_jsvalue!(WriteError);
impl_from_jsvalue!(ClosedStreamError);

fn js_value_to_io_error(err: JsValue) -> std::io::Error {
    std::io::Error::other(format!("{err:?}"))
}
