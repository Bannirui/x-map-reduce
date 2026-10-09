# Architecture

Mermaid diagrams below render directly on GitHub. They describe the current
implementation (persistent master/worker, control/data plane split,
worker-to-worker shuffle, runtime plugin distribution, speculative execution).

## 1. Deployment & control/data plane

```mermaid
flowchart LR
  subgraph Client
    SUB["xmr-submit<br/>(client SDK / CLI)"]
  end

  subgraph Master["x-master (metadata only)"]
    CL["control listener<br/>--listen"]
    DL["data listener<br/>--data-listen (9331)"]
    CO["Coordinator<br/>Scheduler + WorkerRegistry"]
    CL <--> CO
    DL <--> CO
  end

  subgraph WA["x-worker A"]
    RA["reactor + compute pool"]
    DA["data listener"]
    SA[("local map output")]
  end

  subgraph WB["x-worker B"]
    RB["reactor + compute pool"]
    DB["data listener"]
    SB[("local map output")]
  end

  SUB -- "Submit / SubmitResult" --> CL
  SUB -- "upload input + plugin bytes" --> DL
  RA -- "Hello / Ping / Task / Done / Fail / Progress / PluginAck" --> CL
  RB -- "Hello / Ping / Task / Done / Fail / Progress / PluginAck" --> CL
  CO -- "Task / NeedPlugin / Cancel / Stop / Shutdown" --> RA
  RA -- "PullInput / PullPlugin" --> DL
  RB -- "PullInput / PullPlugin" --> DL
  RA <-- "Pull(mapTask, partition) = shuffle" --> DB
  RA -. "serve Pull" .-> SA
  RB -. "serve Pull" .-> SB
```

- **Control plane** (small messages): submit, heartbeat, task scheduling, done/fail.
- **Data plane** (bulk): map input, plugin binaries, and the worker-to-worker shuffle.
- The master never carries bulk bytes; shuffle is peer-to-peer between workers.

## 2. Job lifecycle (happy path)

```mermaid
sequenceDiagram
  participant C as xmr-submit
  participant M as x-master
  participant WA as worker A (map)
  participant WB as worker B (reduce)

  C->>M: Submit(job, reducers, output, inputs[])
  M-->>C: SubmitAck(dataHost, dataPort)
  C->>M: InputBlob*(inputs) + Plugin*(job.so)  [data plane]
  M->>WA: NeedPlugin(hash)
  WA->>M: PullPlugin(hash)  [data plane]
  WA-->>M: PluginAck

  Note over M: dispatch once connected workers are ready

  M->>WA: Task(map i, attempt)
  WA->>M: PullInput(i)  [data plane]
  WA->>M: Progress / Done(map)
  Note over M: reduce barrier: wait for all maps

  M->>WB: Task(reduce r, locations=[map->worker])
  WB->>WA: Pull(map i, partition r)  [data plane, P2P shuffle]
  WB->>M: Result + Done(reduce)

  M-->>C: SubmitResult(output)
  M->>WA: Stop (job end / reset)
  M->>WB: Stop
```

Plus fault handling on the same paths: heartbeat timeout -> worker `Lost` ->
reclaim + retry; a dead map worker's finished maps are invalidated and re-run;
a slow task is duplicated (speculation) and the loser gets `Cancel`.

## 3. Module layers

```mermaid
flowchart BT
  NET["net<br/>ByteBuffer, nonblocking sockets, epoll Poller/EventLoop,<br/>TimerQueue, Notifier(eventfd), ThreadPool"]
  PROTO["protocol<br/>XMRP frame + TLV codec + message structs"]
  MR["mapreduce<br/>Map / Shuffle / Reduce + plugin ABI"]
  RT["runtime<br/>Task, Scheduler, WorkerRegistry, plugin loader"]
  SRV["server<br/>x-master, x-worker"]
  CLI["client<br/>C++ SDK + C ABI + xmr-submit"]

  PROTO --> NET
  RT --> MR
  RT --> PROTO
  SRV --> RT
  SRV --> PROTO
  SRV --> NET
  CLI --> PROTO
  CLI --> NET
```

Reading order: `mapreduce` -> `runtime` -> `net` -> `protocol` -> `server/worker`
-> `server/master` -> `client`, with `tests/` as executable specs.
