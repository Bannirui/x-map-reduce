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

# run the test suite
cd <repo>
ctest --test-dir build --output-on-failure
```

## 2 FEATURE

- [X] Single-process MapReduce
- [X] Multi-threaded MapReduce (parallel Map tasks)
- [ ] Multi-process MapReduce
- [ ] Distributed MapReduce over TCP
- [ ] Fault-tolerant MapReduce
