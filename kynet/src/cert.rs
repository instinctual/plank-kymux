// Project Kyber: cert.rs
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

use crate::error::ConnectionError;

use std::path::Path;

use log::error;
use thiserror::Error;

pub type Certificate = rustls::pki_types::CertificateDer<'static>;
pub type PrivateKey = rustls::pki_types::PrivateKeyDer<'static>;
pub type RootCertStore = rustls::RootCertStore;

pub async fn load_cert_from_pem_file(
    cert_path: impl AsRef<Path>,
) -> Result<Certificate, LoadCertError> {
    let pem_cert = tokio::fs::read(&cert_path).await?;
    let certificate = match rustls_pemfile::read_one(&mut pem_cert.as_ref())? {
        Some(rustls_pemfile::Item::X509Certificate(cert)) => cert,
        _ => {
            error!(
                "{} does not contain a valid certificate",
                cert_path.as_ref().to_string_lossy()
            );
            return Err(LoadCertError::Cert(CertError(
                "Invalid certificate".to_string(),
            )));
        }
    };

    Ok(certificate)
}

pub async fn load_private_key_from_pem_file(
    key_path: impl AsRef<Path>,
) -> Result<PrivateKey, LoadCertError> {
    let pem_key = tokio::fs::read(&key_path).await?;
    let private_key = match rustls_pemfile::read_one(&mut pem_key.as_ref())? {
        Some(rustls_pemfile::Item::Pkcs1Key(key)) => rustls::pki_types::PrivateKeyDer::Pkcs1(key),
        Some(rustls_pemfile::Item::Pkcs8Key(key)) => rustls::pki_types::PrivateKeyDer::Pkcs8(key),
        Some(rustls_pemfile::Item::Sec1Key(key)) => rustls::pki_types::PrivateKeyDer::Sec1(key),
        _ => {
            error!(
                "{} does not contain a valid key",
                key_path.as_ref().to_string_lossy()
            );
            return Err(LoadCertError::Cert(CertError("Invalid key".to_string())));
        }
    };

    Ok(private_key)
}

#[derive(Debug, Error)]
#[error("certificate error: {0}")]
pub struct CertError(pub String);

#[derive(Debug, Error)]
pub enum LoadCertError {
    #[error("IO error")]
    Io(#[from] std::io::Error),
    #[error("Certificate error")]
    Cert(#[from] CertError),
}

impl From<rustls::Error> for CertError {
    fn from(value: rustls::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<rustls::Error> for ConnectionError {
    fn from(value: rustls::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<CertError> for ConnectionError {
    fn from(value: CertError) -> Self {
        Self(value.0)
    }
}
