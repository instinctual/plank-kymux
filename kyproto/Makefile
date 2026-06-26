export RUSTFLAGS=--cfg=web_sys_unstable_apis

build-quinn:
	cargo build --features=kynet/kynet-quinn

build-wtransport:
	cargo build --features=kynet/kynet-wtransport

build-js:
	cargo build --features=kynet/kynet-webtransport-js --target=wasm32-unknown-unknown
