# Chabi - Alternative to Redis / DiceDB

Chabi is a rust based reimplementation of Redis in Rust.

> (Still in early development)

## Benchmarks

On a Hetzner CCX23 machine with 4 vCPU and 16GB RAM here are our numbers around throughput and GET/SET latencies.

### DiceDB

num-clients |	Throughput (ops/sec)  |	GET p50 (ms) | GET p90 (ms) | SET p50 (ms) | SET p90 (ms)
------------|-------------------------|--------------|--------------|--------------|-------------
4	        |   15655	              | 0.227327	 | 0.337919	    | 0.230399	   | 0.339967

### Chabi

num-clients |	Throughput (ops/sec)  |	GET p50 (ms) | GET p90 (ms) | SET p50 (ms) | SET p90 (ms)
------------|-------------------------|--------------|--------------|--------------|-------------
4	        |   35633	              | 0.071167	 | 0.108543	    | 0.072703	   | 0.111103