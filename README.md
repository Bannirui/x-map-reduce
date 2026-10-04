map-reduce
---

c++ implements paper(MapReduce: Simplified Data Processing on Large Clusters)

refer to https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

## 1 QUICK START

```sh
chmod +x ./build.sh
./build.sh
./build/bin/x-map-reduce
```

V1  Single-process MapReduce
        ↓
V2  Multi-threaded MapReduce
        ↓
V3  Multi-process MapReduce
        ↓
V4  Distributed MapReduce over TCP
        ↓
V5  Fault-tolerant MapReduce
