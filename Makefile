
fmt:
	 cargo fmt --all
	
clippy:
	 cargo clippy --all

test:
	 cargo test --all

bench:
	 cargo bench --all

flamegraph_builder:
	 cargo flamegraph --bench builder --no-inline -- --bench

flamegraph_reader:
	 cargo flamegraph --bench reader --no-inline -- --bench
