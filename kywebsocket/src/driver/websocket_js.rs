// Project Kyber: websocket_js.rs
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

use super::{MessageRecvStreamDriver, MessageSendStreamDriver};
use crate::{Connection, MessageRecvStream, MessageSendStream};

use std::cell::RefCell;
use std::io::{Error, ErrorKind, Result};
use std::rc::Rc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use wasm_bindgen::prelude::*;

pub async fn connect(url: &str) -> Result<Connection> {
    let ws = web_sys::WebSocket::new(url).map_err(js_value_to_io_error)?;

    let (tx, rx) = oneshot::channel();

    // The first closure to fire takes the sender out of the Option
    let tx = Rc::new(RefCell::new(Some(tx)));

    let tx2 = tx.clone();
    let on_open = Closure::<dyn FnMut(web_sys::Event)>::new(move |_event: web_sys::Event| {
        if let Some(tx) = tx2.borrow_mut().take() {
            let _ = tx.send(true);
        }
    });
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));

    let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(move |_event: web_sys::Event| {
        if let Some(tx) = tx.borrow_mut().take() {
            let _ = tx.send(false);
        }
    });
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    match rx.await {
        Ok(false) => {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "WebSocket error during opening handshake",
            ))
        }
        Ok(true) => {}
        Err(_) => {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "internal error: sender dropped before signalling",
            ))
        }
    };

    ws.set_onerror(None);
    Ok(new(ws))
}

fn new(ws: web_sys::WebSocket) -> Connection {
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

    let send = MessageSendStream::new(WebSocketJsMessageSendStreamDriver::new(ws.clone()));
    let recv = MessageRecvStream::new(WebSocketJsMessageRecvStreamDriver::new(ws));
    Connection { send, recv }
}

#[derive(Debug)]
struct WebSocketJsMessageSendStreamDriver {
    ws: web_sys::WebSocket,
}

impl WebSocketJsMessageSendStreamDriver {
    fn new(ws: web_sys::WebSocket) -> Self {
        assert!(ws.binary_type() == web_sys::BinaryType::Arraybuffer);
        Self { ws }
    }
}

#[async_trait(?Send)]
impl MessageSendStreamDriver for WebSocketJsMessageSendStreamDriver {
    async fn send(&mut self, msg: Bytes) -> Result<()> {
        self.ws
            .send_with_u8_array(&msg)
            .map_err(js_value_to_io_error)
    }

    async fn close(self: Box<Self>) {
        let _ = self.ws.close();
    }
}

#[derive(Debug)]
struct WebSocketJsMessageRecvStreamDriver {
    ws: web_sys::WebSocket,
    rx: mpsc::UnboundedReceiver<Bytes>,
    _on_message: Closure<dyn FnMut(web_sys::MessageEvent)>,
    _on_close: Closure<dyn FnMut(web_sys::CloseEvent)>,
}

impl WebSocketJsMessageRecvStreamDriver {
    fn new(ws: web_sys::WebSocket) -> Self {
        assert!(ws.binary_type() == web_sys::BinaryType::Arraybuffer);

        let (tx, rx) = mpsc::unbounded_channel();

        let tx = Rc::new(RefCell::new(Some(tx)));

        let tx2 = tx.clone();
        let on_message = Closure::<dyn FnMut(web_sys::MessageEvent)>::new(
            move |event: web_sys::MessageEvent| {
                if let Some(tx) = tx2.borrow_mut().as_mut() {
                    let bytes = event
                        .data()
                        .dyn_into::<js_sys::ArrayBuffer>()
                        .ok()
                        .map(|buf| Bytes::from(js_sys::Uint8Array::new(&buf).to_vec()));
                    if let Some(bytes) = bytes {
                        let _ = tx.send(bytes);
                    }
                }
            },
        );
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

        let on_close =
            Closure::<dyn FnMut(web_sys::CloseEvent)>::new(move |event: web_sys::CloseEvent| {
                // Drop the sender to signal that the connection is closed
                log::info!(
                    "Websocket closed: code: {code} - reason: '{reason}' - wasClean: {wasclean}",
                    code = event.code(),
                    reason = event.reason(),
                    wasclean = event.was_clean()
                );
                *tx.borrow_mut() = None;
            });
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        Self {
            ws,
            rx,
            _on_message: on_message,
            _on_close: on_close,
        }
    }
}

impl Drop for WebSocketJsMessageRecvStreamDriver {
    fn drop(&mut self) {
        self.ws.set_onmessage(None);
        self.ws.set_onclose(None);
    }
}

#[async_trait(?Send)]
impl MessageRecvStreamDriver for WebSocketJsMessageRecvStreamDriver {
    async fn recv(&mut self) -> Result<Option<Bytes>> {
        Ok(self.rx.recv().await)
    }
}

fn js_value_to_io_error(err: JsValue) -> std::io::Error {
    std::io::Error::other(format!("{err:?}"))
}
