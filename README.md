map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh && ctest --test-dir build --output-on-failure
```

Run from `build/bin`: the binaries, the plugins and the copied `asset/` inputs all resolve from there.

### 1.1.1 start master

```sh
cd ./build/bin

./x-coordinator --job word_count \
  --plugin ../lib/word_count.so \
  --workers 3 \
  --reducers 3 \
  --listen 127.0.0.1:9527 \
  --output wc.txt \
  asset/wordCount1.txt asset/wordCount2.txt asset/wordCount3.txt
```

> only the coordinator needs the input files; workers fetch their splits over TCP.
> `--plugin` points at the job's shared object; jobs are no longer compiled in.
> `--workers 3` withholds all work until exactly 3 workers connect, so start 3.

### 1.1.2 start workers

In another shell (also from `build/bin`):

```sh
cd ./build/bin
for i in 1 2 3; do
  ./x-worker --coordinator 127.0.0.1:9527 --plugin ../lib/word_count.so &
done
wait
```

## 2 FEATURE

- [X] V1 Single-process MapReduce
- [X] V2 Multi-threaded MapReduce (parallel Map tasks)
- [X] V3 Multi-process MapReduce
  - [X] V3.0 multi-process Map(`fork`/`exec` one `map_worker` per input file)
  - [X] V3.1 Map+Reduce processes(`reduce_worker` filters `fnv(key) % R`)
  - [X] V3.2 partition intermediate data at the map side(`fnv(key) % R`)
- [X] V4 Distributed MapReduce over TCP
  - [X] V4.0 master on tcp
  - [X] V4.1 data shuffle on tcp
  - [X] V4.2 distributed input data
- [ ] V5 Fault-tolerant MapReduce
  - [ ] V5.0 task attempts
    - idempotent retry
    - task timeouts
    - worker-death recovery
  - [ ] V5.1 speculative execution
    - duplicate slow tasks
    - first attempt wins
  - [ ] V5.2 heartbeat liveness
    - extract worker management into its own class (`WorkerRegistry`: registration, idle/liveness), mirroring how `Scheduler` isolates task scheduling; the coordinator keeps only I/O
- [ ] V6 Job submission—submit arbitrary map/reduce to the master
  - [X] V6.0 native plugin ABI
    - versioned `extern "C"` map/reduce interface
    - worker loads `job.so` with `dlopen`
  - [ ] V6.1 submission+code distribution
    - `SUBMIT`/`PLUGIN` messages
    - plugin shipped once per worker, cached by content hash
  - [ ] V6.2 *(optional)* scripting jobs (Lua/Python) instead of native plugins
- [ ] V7 HA cluster master
  - [ ] V7.0 stateless coordinator: move shuffle data off the master (direct worker-to-worker fetch)
  - [ ] V7.1 persistent cluster master that accepts jobs; the per-job coordinator becomes a restartable driver
  - [ ] V7.2 leader election via an external consensus store (ZooKeeper/etcd/Consul)
- [ ] V8 epoll+reactor
  - [ ] V8.1 [protocl enhanceman](./protocol/PROTOCOL.md)