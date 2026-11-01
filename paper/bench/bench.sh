#!/bin/bash
# ============================================================================
# x-map-reduce 实验驱动脚本
#
# 用于产出 IEEE workshop 论文 Evaluation 章节所需的数据。
#
# 用法:
#   ./bench.sh gen      <size_mb>          生成指定大小的 word-count 数据集
#   ./bench.sh run      <workers> <inputs> 以 N 个 worker 运行一次作业，输出耗时
#   ./bench.sh scaling  <sizes>            可扩展性实验: workers x sizes 矩阵
#   ./bench.sh failure  <workers>          故障恢复实验: 作业中途 kill 一个 worker
#   ./bench.sh clean                       清理运行时目录
#
# 设计要点:
#   - 每次测量使用全新的 worker 数据目录，避免缓存干扰
#   - 结果以 CSV 追加，便于后续画图
#   - 所有进程限制在 127.0.0.1，不暴露公网
# ============================================================================
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="$ROOT/build/bin"
LIB="$ROOT/build/lib"
DATA="${XMR_DATA:-/root/xmr_data}"
RESULT="$ROOT/paper/bench/results"
DS="$DATA/ds"
LOG="$DATA/logs"

MASTER_PORT=9527
DATA_PORT=9331

mkdir -p "$DS" "$LOG" "$RESULT" "$DATA"

# ---------------------------------------------------------------- 工具函数
log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# 等待 master 打印 LISTENING
wait_port() {
    local port=$1 tries=100
    while (( tries-- > 0 )); do
        (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && return 0
        sleep 0.1
    done
    return 1
}

# 记录本次运行启动的子进程, 用 PID 精确清理.
# 说明: pkill -f 在非交互 SSH 下匹配不可靠, 曾导致残留 worker 污染计时,
#       因此改为显式记录 PID 并逐个 wait/kill.
PIDS=()

stop_all() {
    local p
    for p in "${PIDS[@]:-}"; do
        [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null
    done
    for p in "${PIDS[@]:-}"; do
        [[ -n "$p" ]] && wait "$p" 2>/dev/null
    done
    PIDS=()
    # 等监听端口真正释放, 否则下一次 bind 会失败
    local tries=50
    while (( tries-- > 0 )) && wait_port "$MASTER_PORT"; do sleep 0.2; done
    sleep 0.3
}

# ---------------------------------------------------------------- 数据集生成
# 生成有换行的文本。每行 8~12 个词，词表有限以便 reduce 阶段有归并压力。
gen() {
    local size_mb=${1:-100}
    local out="$DS/words_${size_mb}mb.txt"
    if [[ -f "$out" ]]; then
        log "数据集已存在: $out ($(du -h "$out" | cut -f1))"
        echo "$out"
        return 0
    fi
    log "生成 ${size_mb}MB 数据集 -> $out"
    # 参数可用环境变量覆盖:
    #   XMR_VOCAB  词表规模, 决定 reduce 阶段不同 key 的数量
    #   XMR_WPL    每行词数, 决定 map 输出的 key-value 总数
    # 说明: map 输出量 ≈ 行数 x 每行词数, 会远大于输入字节数;
    #       该比值决定 shuffle 压力, 是实验的重要调节变量.
    XMR_VOCAB="${XMR_VOCAB:-2000}" XMR_WPL="${XMR_WPL:-5}" \
    python3 - "$size_mb" "$out" <<'PY'
import os, random, sys
size_mb = int(sys.argv[1]); out = sys.argv[2]
vocab_size = int(os.environ.get("XMR_VOCAB", "2000"))
words_per_line = int(os.environ.get("XMR_WPL", "5"))
target = size_mb * 1024 * 1024
vocab = [f"w{i}" for i in range(vocab_size)]
random.seed(20261101)
written = 0
with open(out, "w", buffering=1 << 20) as f:
    buf = []
    buflen = 0
    while written + buflen < target:
        line = " ".join(random.choices(vocab, k=words_per_line)) + "\n"
        buf.append(line); buflen += len(line)
        if buflen >= (1 << 20):
            f.write("".join(buf)); written += buflen
            buf.clear(); buflen = 0
    if buf:
        f.write("".join(buf)); written += buflen
print(f"{out}|{written}|vocab={vocab_size}|wpl={words_per_line}", file=sys.stderr)
PY
    log "生成完成: $(du -h "$out" | cut -f1)"
    echo "$out"
}

# ---------------------------------------------------------------- 单次运行
# run <workers> <dfs-input...> ; 输出一行为 CSV
run() {
    local workers=$1; shift
    local tag="${1##*/}"; tag="${tag%.txt}"

    stop_all
    local rundir="$DATA/run_$(date +%s%N)"
    mkdir -p "$rundir"
    for ((i = 1; i <= workers; i++)); do mkdir -p "$rundir/blocks$i"; done

    # --- 启动 master ---
    "$BIN/x-master" --listen "$MASTER_PORT" --data-listen "$DATA_PORT" \
        > "$LOG/master.log" 2>&1 &
    PIDS+=($!)
    wait_port "$MASTER_PORT" || { log "master 启动失败"; return 1; }

    # --- 启动 workers ---
    for ((i = 1; i <= workers; i++)); do
        "$BIN/x-worker" --master "127.0.0.1:$MASTER_PORT" \
            --plugin-cache "$rundir/plugins" \
            --data-dir "$rundir/blocks$i" > "$LOG/worker$i.log" 2>&1 &
        PIDS+=($!)
    done
    # 等 worker 注册完成（README 未提供查询接口，用固定等待）
    sleep 2

    # --- 入库 ---
    local t_put_start t_put_end
    t_put_start=$(date +%s.%N)
    local dfs_paths=()
    for f in "$@"; do
        local p; p=$(basename "$f")
        "$BIN/x-dfs-put" put --master "127.0.0.1:$MASTER_PORT" "$f" "$p" \
            > "$LOG/put_$p.log" 2>&1 || { log "put 失败: $f"; return 1; }
        dfs_paths+=("$p")
    done
    t_put_end=$(date +%s.%N)

    # --- 提交作业并计时 ---
    # timeout 防止个别配置意外挂死拖住整个实验
    local t0 t1 rc
    t0=$(date +%s.%N)
    timeout 900 "$BIN/x-submit" --master "127.0.0.1:$MASTER_PORT" \
        --job word_count --plugin "$LIB/word_count.so" \
        --reducers "$workers" --output "$rundir/out.txt" \
        "${dfs_paths[@]}" > "$LOG/submit.log" 2>&1
    rc=$?
    t1=$(date +%s.%N)

    local elapsed put_elapsed
    elapsed=$(echo "$t1 - $t0" | bc)
    put_elapsed=$(echo "$t_put_end - $t_put_start" | bc)

    local lines=0
    [[ -f "$rundir/out.txt" ]] && lines=$(wc -l < "$rundir/out.txt")

    stop_all
    echo "$workers,$tag,${elapsed},${put_elapsed},${lines},${rc}"
}

# ---------------------------------------------------------------- 实验：可扩展性
scaling() {
    local workers_list=${1:-"1 2 4 6"}
    local sizes_list=${2:-"100 200 500"}
    local csv="$RESULT/scaling.csv"
    [[ -f "$csv" ]] || echo "workers,dataset,job_seconds,put_seconds,output_lines,exit_code" > "$csv"

    for size in $sizes_list; do
        local ds; ds=$(gen "$size")
        for w in $workers_list; do
            log "=== workers=$w dataset=${size}MB ==="
            local row
            row=$(run "$w" "$ds")
            log "结果: $row"
            echo "$row" >> "$csv"
            sleep 1
        done
    done
    log "可扩展性实验完成 -> $csv"
    column -s, -t "$csv"
}

# ---------------------------------------------------------------- 实验：故障恢复
failure() {
    local workers=${1:-3}
    local size=${2:-10}
    local csv="$RESULT/failure.csv"
    [[ -f "$csv" ]] || echo "scenario,workers,dataset,job_seconds,kill_after_s,recovered,output_lines,exit_code" > "$csv"

    local ds; ds=$(gen "$size")
    local tag="words_${size}mb"

    # 场景：作业进行中 kill 一个 worker，观察是否能完成
    for kill_after in 1 2 3; do
        stop_all
        local rundir="$DATA/fail_$(date +%s%N)"
        mkdir -p "$rundir"
        for ((i = 1; i <= workers; i++)); do mkdir -p "$rundir/blocks$i"; done

        "$BIN/x-master" --listen "$MASTER_PORT" --data-listen "$DATA_PORT" > "$LOG/master.log" 2>&1 &
        local mp=$!
        PIDS+=("$mp")
        wait_port "$MASTER_PORT" || return 1
        local worker_pids=()
        for ((i = 1; i <= workers; i++)); do
            "$BIN/x-worker" --master "127.0.0.1:$MASTER_PORT" \
                --plugin-cache "$rundir/plugins" \
                --data-dir "$rundir/blocks$i" > "$LOG/worker$i.log" 2>&1 &
            worker_pids+=($!)
        done
        PIDS+=("${worker_pids[@]}")
        sleep 2

        "$BIN/x-dfs-put" put --master "127.0.0.1:$MASTER_PORT" "$ds" "$tag" > "$LOG/put.log" 2>&1

        # 用记录下来的 PID 精确 kill, 避免 pkill -f 误杀/漏杀
        local victim="${worker_pids[$((RANDOM % ${#worker_pids[@]}))]}"
        local t0 t1 rc
        t0=$(date +%s.%N)
        ( sleep "$kill_after"; kill -9 "$victim" 2>/dev/null ) &
        local killer=$!
        timeout 600 "$BIN/x-submit" --master "127.0.0.1:$MASTER_PORT" \
            --job word_count --plugin "$LIB/word_count.so" \
            --reducers "$workers" --output "$rundir/out.txt" "$tag" \
            > "$LOG/submit.log" 2>&1
        rc=$?
        t1=$(date +%s.%N)
        wait "$killer" 2>/dev/null

        local elapsed lines=0 recovered
        elapsed=$(echo "$t1 - $t0" | bc)
        [[ -f "$rundir/out.txt" ]] && lines=$(wc -l < "$rundir/out.txt")
        [[ $rc -eq 0 ]] && recovered=1 || recovered=0

        local row="kill_after_${kill_after}s,$workers,${size}MB,${elapsed},${kill_after},${recovered},${lines},${rc}"
        log "结果: $row"
        echo "$row" >> "$csv"
        stop_all
    done
    log "故障实验完成 -> $csv"
    column -s, -t "$csv"
}

# ---------------------------------------------------------------- 清理
clean() {
    stop_all
    rm -rf "$DATA"/run_* "$DATA"/fail_*
    rm -f "$LOG"/*.log
    log "已清理运行时目录"
}

case "${1:-}" in
    gen)     gen "${2:-100}" ;;
    run)     shift; run "$@" ;;
    scaling) scaling "${2:-}" "${3:-}" ;;
    failure) failure "${2:-3}" "${3:-10}" ;;
    clean)   clean ;;
    *)       sed -n '2,22p' "$0" ;;
esac
