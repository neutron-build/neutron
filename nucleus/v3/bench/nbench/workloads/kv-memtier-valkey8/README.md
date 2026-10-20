# kv-memtier-valkey8

memtier_benchmark GET/SET ratio 1:10 against Valkey 8 (port 6379), 16
threads x 4 clients (64 connections), value size 32 B, key space 1..
10M, key-pattern R:R; pipeline 1 and 16; AOF `appendfsync always` and
`everysec` as separate container-start variants (matched durability
pairs, PLAN-v3 §5.2 rule 3). `--test-time` bounds each execution.

Versions recorded in `versions.txt` (server INFO + memtier --version);
memtier output's Totals block is parsed for ops/sec and percentiles.
