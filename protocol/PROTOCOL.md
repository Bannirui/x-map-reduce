# x-map-reduce网络协议

master与worker之间控制/数据的二进制协议

- 可演进 magic+version协商，未知字段可跳过，加消息/加字段不破坏旧节点
- binary-safe 字符串、二进制一律带长度，不再靠 `\t`/`\n`切分
- 异步友好 `requestId`关联请求与应答，为epoll+reactor打基础
- 可运维 心跳、状态码、分块流式，为后续故障检测预留接口

协议与传输解耦 `net/`只负责字节流转与帧头切分，`protocol/`负责帧语义与编解码

---

## 1 帧Frame

每一条消息=16字节固定头+变长body

所有多字节整数一律**大端**网络序

| offset | size | field | 说明 |
|-------:|-----:|-------|------|
| 0      |   4  |`magic`| 固定 `'X','M','R','P'` (`0x584D5250`) |
| 4      |   1  |`version`    | 协议版本 固定值1 |
| 5      |   1  |`type`       | 消息类型 |
| 6      |   2  |`flags`      | 位标志 |
| 8      |   4  |`requestId`  | 请求/应答关联；单向消息填0 |
| 12     |   4  |`payloadLen` | body字节数 上限是64 MiB |


```
0                                   4     5       6           8
+-----------------------------------+-----+-------------------+
|          magic (4 bytes)          | ver | type  |   flags   |
+-----------------------------------+-----+-------+-----------+
|          requestId (4 bytes)      |       payloadLen (4)    |
+-----------------------------------+-------------------------+
|              body (payloadLen决定多少个字节)                 |
```

约束

- 收到`magic`不匹配 → 判定为非法连接，立即关闭
- `version`不一致 → 走HELLO/HELLO_ACK协商 无法兼容则回`HELLO_ACK`带 `StatusCode::UnsupportedVersion`后关闭
- `payloadLen>kMaxPayloadBytes` → 拒绝 防止内存放大
- 分帧只看这16字节 `net/`不需要知道`type`的含义

### 1.1 flags位

| bit |     名称     |                    含义                   |
|----:|--------------|------------------------------------------|
|  0  | `More`       | 分块未结束 后续还有同逻辑消息 用于大blob流式 |
|  1  | `Compressed` | body已压缩 当前未实现 预留 |
|  2  | `Error`      | 本条为错误应答 body带状态码/原因 |

其余位保留位 用0占位

---

## 2 消息类型`type`

```cpp
enum class MessageType : uint8_t {
    Hello        = 1,   // W -> C
    HelloAck     = 2,   // C -> W
    RequestTask  = 3,   // W -> C
    Task         = 4,   // C -> W
    InputRequest = 5,   // W -> C
    Data         = 6,   // C -> W
    MapOutput    = 7,   // W -> C
    Fetch        = 8,   // W -> C
    Result       = 9,   // W -> C
    Done         = 10,  // W -> C
    Fail         = 11,  // W -> C
    Ping         = 12,  // 双向
    Pong         = 13,  // 双向
    Stop         = 14,  // C -> W  本job结束 复位
    Shutdown     = 15,  // C -> W  worker退出进程
    Submit       = 16,  // client -> master  提交任务(含输入路径/输出路径)
    SubmitAck    = 17,  // master -> client  受理结果
    SubmitResult = 18,  // master -> client  job执行结果
    Pull         = 19,  // worker -> worker  拉某个map任务的某个分区
    DataAddress  = 20,  // worker -> master  上报数据面监听端口
};
```

新增类型时只追加编号 不复用 不重排 未知`type`在握手期视为协议错误 运行期可由上层决定忽略或断开

---

## 3 Body编码TLV

body是若干字段的顺序拼接，每个字段：

| size | field | 说明 |
|-----:|-------|------|
| 2 | `fieldId`  | 字段编号，消息内唯一 |
| 1 | `wireType` | `0=Varint` `1=Bytes` `2=Fixed64` |
| 1 | `reserved` | 保留，必须为 0 |
| 4 | `len`      | 后面 value 的字节数 |
| `len` | `value` | 字段值 |

```
+----------+----------+----------+------------------+-----------+
| fieldId  | wireType | reserved |       len        |   value   |
+----------+----------+----------+------------------+-----------+
```

设计要点

- **统一带`len`**: 即使 Varint 也带长度，使解析方可以无条件跳过任何字段（前向兼容）
- **跳过未知`fieldId`**: 解析器不认识的字段直接 `len` 跳过，不报错
- **Varint**: 无符号LEB128，`len`=实际字节数。用于整数，节省小值空间
- **Bytes**: 长度前缀的字节串，用于`string`/blob，天然binary-safe
- **Fixed64**: 固定8字节大端，用于非范围压缩的定长值（当前基本不用，预留）
- `wireType`与调用方取值类型不匹配 → 解析错误（`InvalidArgument`）
- 同一`fieldId`重复出现: **后出现者覆盖前者**（defined behavior，便于兼容旧客户端）

TLV只是body的容器 每条消息用哪些`fieldId`由注册表规定

---

## 4 消息注册表

`fieldId`在**每条消息内**编号（不同消息可复用同一编号）未列出的编号保留

`kind`字段统一使用`WorkKind` `None = 0` `Map = 1<<0 = 1` `Reduce = 1<<1 = 2` 它是**单选**（用`==`判断 不是位掩码）必填消息中`None`视为非法

### HELLO (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `protocolVersion` | Varint |
| 2 | `workerId`        | Bytes (string) |
| 3 | `capabilities`    | Varint (bitmask) |
| 4 | `pid`             | Varint (可选，调试) |

### HELLO_ACK (C→W)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `protocolVersion` | Varint |
| 2 | `sessionId`       | Bytes (string) |
| 3 | `capabilities`    | Varint |
| 4 | `statusCode`      | Varint |
| 5 | `reason`          | Bytes (string) |

### REQUEST_TASK (W→C)
无字段（worker 身份由连接 / session 关联）

### TASK (C→W)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `kind`     | Varint (`1=Map`, `2=Reduce`；`0=None` 表示未设置，必填消息中非法) |
| 2 | `taskId`   | Varint |
| 3 | `job`      | Bytes (string) |
| 4 | `reducers` | Varint |
| 5 | `maps`     | Varint |
| 6 | `input`    | Bytes (string，仅 map) |

`requestId`对应之前的`REQUEST_TASK`

### INPUT_REQUEST (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `taskId` | Varint |

### DATA (C→W，应答 INPUT_REQUEST / FETCH)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `offset`  | Varint |
| 2 | `total`   | Varint (可选，总长度) |
| 3 | `payload` | Bytes |

分块 `flags.More=1`表示后面还有 `requestId`回显对应请求。

### MAP_OUTPUT (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `mapTask`   | Varint |
| 2 | `partition` | Varint |
| 3 | `offset`    | Varint |
| 4 | `payload`   | Bytes |

### FETCH (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `mapTask`   | Varint |
| 2 | `partition` | Varint |
| 3 | `offset`    | Varint (断点续传) |

### RESULT (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `reduceTask` | Varint |
| 2 | `offset`     | Varint |
| 3 | `payload`    | Bytes |

### DONE (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `kind`   | Varint |
| 2 | `taskId` | Varint |

### FAIL (W→C)
| id | 字段 | 类型 |
|---:|------|------|
| 1 | `kind`       | Varint |
| 2 | `taskId`     | Varint |
| 3 | `statusCode` | Varint |
| 4 | `reason`     | Bytes (string) |

### PING/PONG (双向)
| id | 字段 | 类型 |
|---:|---------|--------|
| 1  | `nonce` | Varint |

### STOP (C→W)
| id | 字段 | 类型 |
|---:|----------|----------------|
| 1  | `reason` | Bytes (string) |

---

## 5 状态码`StatusCode`

```cpp
enum class StatusCode : uint32_t {
    Ok                = 0,
    Unknown           = 1,
    InvalidArgument   = 2,
    NotFound          = 3,
    Internal          = 4,
    Canceled          = 5,
    Unavailable       = 6,
    UnsupportedVersion= 7,
};
```

用于`HELLO_ACK.statusCode`、`FAIL.statusCode` 可重试性由上层按码判定

---

## 6 时序

```
worker                              master
  |------ HELLO(ver, workerId) ------->|
  |<----- HELLO_ACK(ver, session, Ok) -|
  |------ REQUEST_TASK (id=1) -------->|
  |<----- TASK(reply id=1, MAP 0) -----|
  |------ INPUT_REQUEST(id=2, task=0)->|
  |<----- DATA(reply id=2, offset=0)---|
  |         (执行 map, 得到 R 个分区)   |
  |------ MAP_OUTPUT(t=0, p=0..R-1) -->|
  |------ DONE(MAP, 0) --------------->|
  |------ REQUEST_TASK(id=3) --------->|
  |<----- TASK(reply id=3, REDUCE 0)---|
  |------ FETCH(id=4, map=0, part=0) ->|
  |<----- DATA(reply id=4) ------------|
  |         (shuffle + reduce)         |
  |------ RESULT(reduce=0, payload) -->|
  |------ DONE(REDUCE, 0) ------------>|
  |<----- STOP ------------------------|
```

`requestId`让`DATA`能唯一对应到某次`INPUT_REQUEST`/`FETCH` 这也是未来一条连接上pipeline 多个请求的前提