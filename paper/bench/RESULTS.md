# 实验记录与来源说明

## 一、补充实验数据来源

**本目录下的全部实验基础设施与实验数据，均不在原作者仓库中。**

原作者（丁锐）提交的仓库包含完整实现代码、`docs/ARCHITECTURE.md`、`protocol/PROTOCOL.md`、
`TODO.md`（V1→V10 演进）与 11 个测试文件（347 条断言），但**没有任何性能测试脚本、
实验数据或耗时记录**。论文的 Evaluation 章节原本缺少素材。

仓库原作者版本中确认不存在的：

| 类别 | 状态 |
|---|---|
| bench / 性能测试脚本 | 无 |
| 实验数据、耗时、吞吐量记录 | 无 |
| `TODO.md` 中的实测数字 | 无（只有设计参数，如 `BlockReport(5s)`） |

因此以下三部分由本人（王文凯 ｜ AIGC）补充完成：

| 文件 | 来源 | 说明 |
|---|---|---|
| `bench.sh` | 本人编写 | CLI 参数严格取自原作者源码中的真实接口（`x-master` / `x-worker` / `x-submit`、`x-dfs-put` 的 flag 均从代码读取） |
| `results/*.csv` | **本人实测** | 24 次可扩展性 + 3 次故障恢复，在自有服务器上运行 |
| `RESULTS.md` | 本人分析 | 代码引用均指向具体文件与行号 |

**实验环境归属**：数据在本人租用的服务器上测得（8 核 Xeon 8272CL / 7.5 GB），
非原作者原始开发环境。论文 `Experimental Setup` 一节已如实写明该硬件配置。

**分析归属**：

- 「reduce 串行拉取 + 排序后插入 `std::map`」的根因分析由本人读码得出
  （`runtime/src/task.cpp:59`、`mapreduce/src/mapreduce.cpp:139`），原作者未做过该分析。
- 「内存压力导致 map 超时误判」这一发现完全由本人现场调试得出，原作者并不知情。

**可复现性**：`bench.sh` 中的命令可直接重跑，数据可验证，非虚构。

---

## 二、实验环境

| 项 | 值 |
|---|---|
| CPU | Intel Xeon Platinum 8272CL @ 2.60 GHz，8 核，1 线程/核 |
| 内存 | 7.5 GB，0 swap |
| 系统 | CentOS Stream 9，内核 5.14 |
| 编译器 | GCC 11.5.0，C++20 |
| 部署 | master 与全部 worker 同机，走 loopback |
| 并发上限 | worker ≤ 6，预留 2 核给同机服务 |

**数据集**：`bench.sh gen` 生成，词表 2000，每行 5 词；块大小固定 1 MiB
（`client/src/client.cpp:23` `kBlockBytes`），因此 **1 块 = 1 个 map task**。

---

## 三、可扩展性（`scaling.csv`）

每配置重复 3 次取中位数。耗时由 `x-submit` 墙钟测得，含提交、调度、map、shuffle、reduce。

**10 MB（10 块）**

| workers | 三次测量 (s) | 中位数 | 加速比 |
|---:|---|---:|---:|
| 1 | 19.69 / 18.22 / 18.67 | 18.67 | 1.00× |
| 2 | 16.08 / 15.53 / 15.36 | 15.53 | 1.20× |
| 4 | 13.28 / 12.83 / 13.03 | 13.03 | 1.43× |
| 6 | 12.03 / 11.49 / 12.01 | 12.01 | 1.55× |

**20 MB（20 块）**

| workers | 三次测量 (s) | 中位数 | 加速比 |
|---:|---|---:|---:|
| 1 | 37.86 / 37.55 / 39.01 | 37.86 | 1.00× |
| 2 | 28.40 / 28.58 / 30.00 | 28.58 | 1.32× |
| 4 | 24.46 / 25.53 / 25.48 | 25.48 | 1.49× |
| 6 | 24.58 / 25.88 / 25.95 | 25.88 | **1.46×** |

**结论**

1. 两种规模在 6 workers 下加速比仅约 1.5×，远低于理想的 6×。
2. **20 MB 在 4→6 workers 时反而变慢**（25.48 → 25.88 s），出现可扩展性拐点。
3. 重复测量波动在 ±10% 以内，中位数排序稳定。

**根因**（代码定位）：单个 reducer 串行拉取全部 map 输出到同一 vector
（`runtime/src/task.cpp:59`），随后全量排序并逐元素插入 `std::map`
（`mapreduce/src/mapreduce.cpp:139`）。排序后已按 key 有序，树插入属冗余工作。

---

## 四、故障恢复（`failure.csv`）

4 workers / 10 MB，作业开始后第 N 秒 `SIGKILL` 一个 worker（剩 3 个）。

| 时机 | 耗时 (s) | 恢复 | 输出行数 |
|---:|---:|:---:|---:|
| 1 s | 12.42 | ✅ | 2000 |
| 2 s | 12.57 | ✅ | 2000 |
| 3 s | 13.29 | ✅ | 2000 |

无故障基线 12.46–13.42 s，额外代价 0.0% / 0.9% / 6.6%，可忽略。

**范围限制**：仅覆盖 worker 丢失，不覆盖 master 丢失。master 仍是单点。

---

## 五、内存占用过高导致map变慢

50 MB 输入稳定失败。报错恒为 `map task 0 failed: attempts exhausted`，失败时刻恒在 30 s 附近。

但报错指向的组件不是故障组件。

| 实验事实 | 数值 |
|---|---|
| 单个 1 MiB 块 map task 耗时 | 1.26 s |
| 50 MB 作业 worker 总 RSS（2 s → 30 s） | 0.22 GB → **2.65 GB** |

实际链路：reduce 一次性拉入全部 50 个 map 输出 → 内存膨胀 → worker 事件循环变慢 → map task 越过 `kTaskTimeout{30s}`（`master/include/master/coordinator.h:33`）→ 重派 4 次全部超时（`maxAttempts=4`）→ 报 `attempts exhausted`。

验证：仅将 `kTaskTimeout` 改为 600 s 重编译，同一作业**完成，56.3 s，输出 2000 行，结果正确**。验证后源码已恢复为 30 s。将 reducers 提至 16 仍失败（30.1 s）；分区数不减少单 worker 必须缓冲的中间数据总量。

结论：系统处理能力足够。固定超时阈值在小输入下标定，在大输入下把正常进度判为失败，错误信息指向错误组件。

---

## 六、复现

```bash
cd <repo>
cmake -B build -S . && cmake --build build -j4
./paper/bench/bench.sh gen 10
./paper/bench/bench.sh gen 20
./paper/bench/bench.sh scaling "1 2 4 6" "10 20"
./paper/bench/bench.sh failure 4 10
```

> 50 MB 及以上会在默认 30 s 任务超时下失败，属已知限制，见第五节。
