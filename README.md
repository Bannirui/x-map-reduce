map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh
```

and i've provided few samples for u

```sh
# examples (assets are copied next to the binaries)
cd ./build/bin
./word_count
./sum

# multi-process (V3.2): map_worker per input partitions by fnv(key)%R; R reduce_worker processes
./coordinator --job word_count --reducers 3 --work-dir /tmp/mr --output out.txt asset/wordCount1.txt asset/wordCount2.txt asset/wordCount3.txt
./coordinator --job sum --reducers 3 --work-dir /tmp/mr --output sum.txt asset/sum.txt

# run the test suite
cd <repo>
ctest --test-dir build --output-on-failure
```

## 2 FEATURE

- [X] V1 Single-process MapReduce
- [X] V2 Multi-threaded MapReduce (parallel Map tasks)
- [X] V3 Multi-process MapReduce
  - [X] V3.0: multi-process Map (`fork`/`exec` one `map_worker` per input file)
  - [X] V3.1: Map + Reduce processes (`reduce_worker` filters `fnv(key) % R`)
  - [X] V3.2: partition intermediate data at the map side (`fnv(key) % R`)
- [ ] V4 Distributed MapReduce over TCP
- [ ] V5 Fault-tolerant MapReduce
