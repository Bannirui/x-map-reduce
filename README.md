map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh

# word count
cd ./build/bin
./wordCount

# calculate
cd ./build/bin
./sum
```

## 2 FEATURE

- [X] Single-process MapReduce
- [ ] Multi-threaded MapReduce
- [ ] Multi-process MapReduce
- [ ] Distributed MapReduce over TCP
- [ ] Fault-tolerant MapReduce
