map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh


# run the test suite
cd <repo>
ctest --test-dir build --output-on-failure
```

and i've provided few samples for u

### 1.1 only one binary

```sh
cd ./build/bin

./word_count
./sum
```

### 1.2 master and worker

#### 1.2.1 start master

```sh
./coordinator --job word_count \
  --workers 3 \
  --reducers 3 \
  --listen 127.0.0.1:9527 \
  --work-dir mr/wc \
  --output wc.txt \
  asset/wordCount1.txt asset/wordCount2.txt asset/wordCount3.txt
```

#### 1.2.1 start master

start workers

```sh
./worker --coordinator 127.0.0.1:9527
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
