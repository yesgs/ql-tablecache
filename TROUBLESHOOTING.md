# 故障排查指南

本文档帮助您诊断和解决 tablecache 常见问题。

## 快速诊断

### 检查 Redis 键状态

```bash
# 连接到 Redis
redis-cli

# 查看快照键（替换 your-namespace）
HGETALL "tablecache:{your-namespace}:snapshot"
# 返回: version (SHA-256), body (JSON 快照)

# 查看门控键（刷新租约）
GET "tablecache:{your-namespace}:gate"
TTL "tablecache:{your-namespace}:gate"
# 返回: 当前持有租约的令牌（或 nil），剩余 TTL（秒）
```

### 检查应用指标

```go
metrics := cache.Metrics().GetMetrics()
fmt.Printf("刷新成功: %d, 失败: %d\n", metrics.RefreshSuccess, metrics.RefreshFailure)
fmt.Printf("最近刷新: %v (%v ago)\n", metrics.LastRefreshTime, time.Since(metrics.LastRefreshTime))
fmt.Printf("快照: %d 行, %d 字节, 版本 %s\n", 
    metrics.SnapshotRows, metrics.SnapshotBytes, metrics.SnapshotVersion)
fmt.Printf("轮询: %d 次, 错误 %d 次\n", metrics.PollAttempts, metrics.PollErrors)
```

## 常见问题

### 1. 所有实例读到旧数据

**症状**: `LastRefreshTime` 停止更新，快照版本长时间不变

**可能原因**:
- MySQL 查询失败（权限、超时、表不存在）
- Loader 死锁或无限循环
- Redis 门控键卡住（所有实例认为有人在刷新）

**排查步骤**:

```bash
# 1. 检查门控键是否有 TTL
redis-cli TTL "tablecache:{namespace}:gate"
# 如果返回 -1（永不过期），说明键损坏了

# 2. 手动清除门控键，强制下次轮询尝试刷新
redis-cli DEL "tablecache:{namespace}:gate"

# 3. 或在应用代码中调用
err := cache.ForceRefresh(context.Background())
```

**预防措施**:
- 设置合理的 `LoadTimeout`（默认 15 秒）
- Loader 必须遵守传入的 context
- 监控 `RefreshFailure` 指标并告警

### 2. 新实例启动卡在 WaitReady

**症状**: `WaitReady(ctx)` 超时，日志显示 "poll: connection refused" 或 "no such host"

**可能原因**:
- Redis 连接配置错误
- Redis 完全不可用且没有已发布的快照
- 网络隔离

**排查步骤**:

```go
// 1. 测试 Redis 连接
ctx := context.Background()
if err := rdb.Ping(ctx).Err(); err != nil {
    log.Fatal("Redis 不可达:", err)
}

// 2. 检查是否已有快照
version, err := rdb.HGet(ctx, "tablecache:{namespace}:snapshot", "version").Result()
if err == redis.Nil {
    log.Println("Redis 中没有快照，需要至少一个实例成功刷新")
} else if err != nil {
    log.Fatal("Redis 查询失败:", err)
} else {
    log.Println("Redis 中已有版本:", version)
}
```

**解决方案**:
- 确保 Redis 可达且 ACL 允许必要命令（`HGET`, `HSET`, `GET`, `PSETEX`, `DEL`）
- 冷启动时，至少一个实例需要成功查询 MySQL 并发布快照
- 如果 MySQL 也故障，考虑从备份恢复 Redis 快照

### 3. 快照超过 MaxBytes 无法发布

**症状**: 日志显示 "encoded snapshot has X bytes; MaxBytes is Y"

**解决方案**:

```go
// 1. 短期: 提高限制
cache, err := tablecache.New(rdb, tablecache.Options{
    Namespace: "prod:config:v1",
    MaxBytes:  512 << 20, // 512 MiB
}, loader)

// 2. 长期: 拆分数据集或优化 Loader
func loader(ctx context.Context) (map[int64]Config, error) {
    // 只查询必要的字段，避免 TEXT/BLOB 大字段
    rows, err := db.QueryContext(ctx, 
        "SELECT id, name, status FROM config WHERE enabled = 1")
    // 而不是 SELECT *
}

// 3. 设置软限制提前告警
cache, err := tablecache.New(rdb, tablecache.Options{
    Namespace: "prod:config:v1",
    MaxBytes:  256 << 20,
    WarnBytes: 200 << 20, // 达到 200 MiB 时告警
    OnWarn: func(err error) {
        log.Println("警告:", err)
        // 发送监控告警
    },
}, loader)
```

### 4. 数据不一致：同一请求读到混合版本

**症状**: 一个请求内多次 `cache.Get()` 返回不同版本的数据

**原因**: 刷新期间新快照安装，每次 `Get()` 都读最新指针

**解决方案**: 使用 `Snapshot()` 固定版本

```go
// 错误示例
func handleRequest(cache *tablecache.Cache[int64, Config]) {
    a, _ := cache.Get(1) // 可能读到版本 N
    // ... 此时刷新完成，切换到版本 N+1
    b, _ := cache.Get(2) // 读到版本 N+1
    // a 和 b 可能来自不同版本
}

// 正确示例
func handleRequest(cache *tablecache.Cache[int64, Config]) {
    snapshot := cache.Snapshot() // 固定当前版本
    a, _ := snapshot.Get(1)
    b, _ := snapshot.Get(2)
    // a 和 b 一定来自同一版本
}
```

### 5. Redis 内存压力导致快照被驱逐

**症状**: 
- 新实例 `WaitReady` 超时
- 日志显示 "Redis snapshot checksum mismatch"
- Redis 返回 "OOM command not allowed"

**排查步骤**:

```bash
# 检查 Redis 内存使用
redis-cli INFO memory | grep used_memory_human

# 检查驱逐策略
redis-cli CONFIG GET maxmemory-policy
# 推荐: noeviction 或 allkeys-lru

# 检查快照大小
redis-cli HSTRLEN "tablecache:{namespace}:snapshot" body
```

**解决方案**:
- 为快照预留足够内存（至少 2 倍 MaxBytes，考虑多数据集）
- 设置 `maxmemory-policy noeviction` 拒绝写入而非驱逐
- 或使用 `volatile-lru` 但给快照键设置足够长的 TTL

### 6. 刷新频率过高或过低

**问题**: 变更可见延迟不符合预期

**调整参数**:

```go
cache, err := tablecache.New(rdb, tablecache.Options{
    Namespace: "prod:config:v1",
    RefreshInterval: 30 * time.Second, // 变更后 30 秒内可见
    PollInterval:    1 * time.Second,  // 每秒轮询 Redis（带抖动）
}, loader)

// 注意权衡:
// - RefreshInterval 过短: MySQL 压力大，Redis 带宽高
// - RefreshInterval 过长: 变更可见延迟高
// - PollInterval 过短: Redis QPS 高（实例数 × 1/PollInterval）
// - PollInterval 过长: 变更传播慢
```

**强制立即刷新** (用于紧急配置变更):

```go
if err := cache.ForceRefresh(ctx); err != nil {
    log.Println("强制刷新失败:", err)
}
// 下次 PollInterval 内会尝试刷新
```

### 7. 租约过期导致重复刷新

**症状**: 同一轮刷新 MySQL 被查询多次，`RefreshSuccess` 异常高

**原因**: `LoadTimeout` 设置过小，查询未完成租约就过期

**解决方案**:

```go
// 1. 分析实际查询耗时
start := time.Now()
rows, err := db.Query("SELECT ...")
log.Printf("查询耗时: %v", time.Since(start))

// 2. 设置合理的 LoadTimeout（实际耗时 × 2 以上）
cache, err := tablecache.New(rdb, tablecache.Options{
    Namespace:   "prod:config:v1",
    LoadTimeout: 30 * time.Second, // 如果查询需要 10 秒
}, loader)

// 3. 优化慢查询
// - 添加索引
// - 减少 JOIN
// - 只查询必要字段
```

## 日志错误码说明

### `poll: context deadline exceeded`
Redis 操作超过 `RedisTimeout`。检查网络延迟或提高超时。

### `load: no time left in refresh lease`
获得租约时剩余时间不足执行加载。说明网络延迟高或其他实例释放租约过晚。

### `lease lost before publication`
刷新完成前租约过期，其他实例可能已发布新版本。通常是 `LoadTimeout` 过小。

### `source snapshot keys must survive a JSON round-trip`
Loader 返回的某些键经过 JSON 编码/解码后改变。检查自定义 `MarshalJSON/UnmarshalJSON` 实现。

### `Redis snapshot exceeds MaxBytes`
远程快照大于本地 `MaxBytes`。统一所有实例的配置或提高限制。

## 监控最佳实践

### Prometheus 指标示例

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    refreshSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "tablecache_refresh_success_total",
        Help: "Total number of successful refreshes",
    }, []string{"namespace"})

    refreshFailure = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "tablecache_refresh_failure_total",
        Help: "Total number of failed refreshes",
    }, []string{"namespace"})

    lastRefreshTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "tablecache_last_refresh_timestamp_seconds",
        Help: "Unix timestamp of last successful refresh",
    }, []string{"namespace"})

    snapshotSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
        Name: "tablecache_snapshot_bytes",
        Help: "Size of current snapshot in bytes",
    }, []string{"namespace"})
)

// 定期采集指标
go func() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for range ticker.C {
        m := cache.Metrics().GetMetrics()
        refreshSuccess.WithLabelValues(namespace).Add(float64(m.RefreshSuccess))
        refreshFailure.WithLabelValues(namespace).Add(float64(m.RefreshFailure))
        lastRefreshTime.WithLabelValues(namespace).Set(float64(m.LastRefreshTime.Unix()))
        snapshotSize.WithLabelValues(namespace).Set(float64(m.SnapshotBytes))
    }
}()
```

### 关键告警规则

```yaml
# 超过 5 分钟未刷新
- alert: TableCacheStale
  expr: time() - tablecache_last_refresh_timestamp_seconds > 300
  annotations:
    summary: "{{ $labels.namespace }} 超过 5 分钟未刷新"

# 刷新失败率超过 50%
- alert: TableCacheHighFailureRate
  expr: rate(tablecache_refresh_failure_total[5m]) / rate(tablecache_refresh_success_total[5m]) > 0.5
  annotations:
    summary: "{{ $labels.namespace }} 刷新失败率过高"

# 快照接近大小限制
- alert: TableCacheNearLimit
  expr: tablecache_snapshot_bytes / 268435456 > 0.9  # 256 MiB 的 90%
  annotations:
    summary: "{{ $labels.namespace }} 快照大小接近限制"
```

## 性能调优

### 减少内存使用

```go
// 1. 使用值类型而非指针
type Config struct {
    ID      int64  // 而非 *int64
    Name    string
    Enabled bool
}

// 2. 避免嵌套 map 和 slice（会增加 GC 压力）
type Config struct {
    Tags []string  // 如果必需，考虑改为位域或预分配
}

// 3. 复用 Loader 的数据库连接
db.SetMaxOpenConns(10)
db.SetConnMaxLifetime(time.Hour)
```

### 优化读取性能

```go
// 已经是最优：Get() 是原子指针 + map 查找
// 基准测试: 1.88 ns/op, 0 allocs/op

// 避免不必要的快照复制
func bad(cache *tablecache.Cache[int, string]) {
    for i := 0; i < 1000; i++ {
        s := cache.Snapshot() // 每次都读原子指针
        v, _ := s.Get(i)
        _ = v
    }
}

func good(cache *tablecache.Cache[int, string]) {
    s := cache.Snapshot() // 只读一次
    for i := 0; i < 1000; i++ {
        v, _ := s.Get(i)
        _ = v
    }
}
```

## 需要帮助？

如果本文档未能解决您的问题，请：

1. 收集日志中的所有错误信息
2. 运行诊断脚本收集 Redis 状态
3. 记录应用指标快照
4. 在项目 issue 中提供上述信息
