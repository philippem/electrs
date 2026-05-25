export RUST_BACKTRACE=1
export MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0
./target/release/electrs --http-addr 0.0.0.0:3001 \
			 --monitoring-addr 0.0.0.0:4224 \
			 --db-block-cache-mb=4096 \
                         --cache-index-filter-blocks \
			 --db-parallelism=20 \
			 --db-write-buffer-size-mb=128 \
			 --db-target-file-size-mb=128 \
			 --db-soft-pending-compaction-gb=8 \
			 --db-hard-pending-compaction-gb=32 \
			 --initial-sync-batch-size=100 \
			 --initial-sync-l0-backpressure-trigger=64 \
			 --daemon-parallelism=4 \
			 --daemon-rpc-addr 0.0.0.0:8332 --electrum-rpc-addr 0.0.0.0:50001 \
			 --db-dir /mnt/quattro/electrs/bitcoin-mainnet \
			 --timestamp \
			 --cookie rpcuser:rpcpassword \
			 --network mainnet \
			 --cors '*' \
			 # -vvv \
			 2>&1 | tee electrs.log
