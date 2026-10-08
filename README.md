map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to [Jeff的MapReduce论文](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh && ctest --test-dir build --output-on-failure
```

Run from `build/bin`: the binaries and the copied `asset/` inputs all resolve from there.

### 1.1.1 start master

```sh
cd ./build/bin

./x-master --listen 127.0.0.1:9527 --data-listen 127.0.0.1:9331
```

> the master is a persistent server: it only listens and waits for a job to be
> submitted via `xmr-submit`. It prints `LISTENING <host:port>` (control plane)
> and `DATA_LISTENING <host:port>` (data plane) once ready.
>
> all bulk traffic (map input, plugin, shuffle) goes over the data plane; the
> control plane only carries small messages. `--data-listen <host:port>` pins the
> data port (default: control host with port 9331).

### 1.1.2 start workers

In another shell (also from `build/bin`), start the workers (they stay connected
across jobs):

```sh
cd ./build/bin
for i in 1 2 3; do
  ./x-worker --master 127.0.0.1:9527 &
done
wait
```

> workers no longer need the plugin up front: the master ships the `.so` to each
> worker at submit time (cached by content hash, loaded with `dlopen`).
> `--plugin-cache <dir>` overrides the cache dir (default `~/.cache/xmr/plugins`).

### 1.1.3 submit a job

In a third shell (also from `build/bin`):

```sh
cd ./build/bin

./xmr-submit --master 127.0.0.1:9527 \
  --job word_count \
  --plugin ../lib/word_count.so \
  --reducers 3 \
  --output wc.txt \
  asset/wordCount1.txt asset/wordCount2.txt asset/wordCount3.txt
```

> the client reads the input files and uploads them to the master (data plane);
> workers fetch their splits over TCP. The master does not keep them on disk.
> `--plugin` is the client's local `.so`; it is uploaded to the master and
> distributed to the workers (the master does not need it on disk).
> the master manages workers, so no worker count is needed: it starts once the
> connected workers have loaded the plugin.
> add `--shutdown` to stop the master (and its workers) after the job.
> submit again to run another job on the same running master/workers.

## 2 ARCHITECTURE

- `x-master` is a persistent coordination server. It owns job metadata only
  (scheduling, worker liveness) and never carries bulk data.
- `x-worker` is a persistent worker. It runs map/reduce on a thread pool so the
  reactor keeps heartbeating, and it keeps map output locally for shuffle.
- `xmr-submit` is the client: it submits a job and uploads the job plugin.
- **Control / data plane split**: small messages (submit, heartbeat, task
  scheduling, done/fail) go to the control port (`--listen`); bulk traffic (map
  input, plugin binaries, shuffle) goes to the data port (`--data-listen`,
  default `9331`).
- **Shuffle is worker-to-worker**: map workers keep their output; reduce workers
  pull partitions directly from map workers. The master only hands out the
  "which worker to pull which partition" metadata.
- **Plugins** are shipped by content hash, cached per worker (atomic write +
  `dlopen`), and only sent to workers that do not have them.
- **Fault tolerance**: heartbeat/timeouts, task retry with attempts, and
  re-running completed maps whose worker died before reduce pulled them.

## 3 FEATURE

refer to [TODO](./TODO.md)