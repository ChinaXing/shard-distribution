Shard distribution
===================
> Build A Matrix for shards distribution tolerance single node failure

Usage
========
cargo run -- --help

- example

```
## 1. print distribution as matrix for 153 shards and 18 server nodes. ( -m 153 18 )
## 2. write dotgraph to a.f, can be render by dot2graph etc. ( -g a.f )
## 3. dump a java array to copy and paste to JAVA code to used. ( -j )
## 4. fail node is first one. ( -f 0 )
cargo run -- -m -f 0 -g a.f 153 18 -j
```
