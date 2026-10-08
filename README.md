map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to [Jeff的MapReduce论文](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh && ctest --test-dir build --output-on-failure
```

Run from `build/bin`: the binaries, the plugins and the copied `asset/` inputs all resolve from there.

### 1.1.1 start master

```sh
cd ./build/bin

./x-master --listen 127.0.0.1:9527
```

> the master is a persistent server: it only listens and waits for a job to be
> submitted via `xmr-submit`. It prints `LISTENING <host:port>` once ready.

### 1.1.2 start workers

In another shell (also from `build/bin`), start the workers (they stay connected
across jobs):

```sh
cd ./build/bin
for i in 1 2 3; do
  ./x-worker --master 127.0.0.1:9527 --plugin ../lib/word_count.so &
done
wait
```

> `--plugin` points at the job's shared object; jobs are no longer compiled in.
> (Runtime plugin distribution to workers is still TODO, so point each worker at
> the `.so` for now.)

### 1.1.3 submit a job

In a third shell (also from `build/bin`):

```sh
cd ./build/bin

./xmr-submit --master 127.0.0.1:9527 \
  --job word_count \
  --workers 3 \
  --reducers 3 \
  --output wc.txt \
  asset/wordCount1.txt asset/wordCount2.txt asset/wordCount3.txt
```

> only the master needs the input files; workers fetch their splits over TCP.
> `--workers 3` withholds all work until exactly 3 workers have connected.
> add `--shutdown` to stop the master (and its workers) after the job.
> submit again to run another job on the same running master/workers.

## 2 FEATURE

refer to [TODO](./TODO.md)