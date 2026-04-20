.PHONY: build release test test-unit test-integration fmt lint check clean coverage

build:
	cargo build

release:
	cargo build --release

test:
	cargo nextest run --workspace

test-unit:
	cargo nextest run --workspace --lib

test-integration:
	cargo nextest run --workspace --test repository_test

fmt:
	cargo +nightly fmt --all

fmt-check:
	cargo fmt --check --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

check: fmt-check lint test

clean:
	cargo clean

coverage:
	cargo llvm-cov --workspace --lib --test repository_test --lcov --output-path lcov.info
