# tablecache

让多台 ECS 共用一次 MySQL 配置查询。每个 Go 进程持有一份按主键索引的完整快照，业务请求通过 `cache.Get(id)` 读取。刷新周期默认一分钟。服务沿用现有 MySQL 只读账号和 Redis，无需改表、改写入方或部署常驻的刷新服务。

## 刷新过程

1. 每个进程启动一个后台协程。它立即访问 Redis，之后约每 2 秒检查一次。各实例使用相同的命名空间，轮询时间带 ±20% 抖动。轮询脚本只读版本、按需返回正文并检查刷新是否到期。实例处理完已有快照后，再用独立的原子操作尝试取得刷新权。
2. 当轮取得刷新权的进程调用业务提供的 Loader。Loader 查询整张配置表的必要字段，按主键构建 map。库校验键可无损往返 JSON，并计算快照的 SHA-256。内容未变且 Redis 正文完整时，只更新下次刷新时间。正文缺失或损坏时，刷新者重新发布完整快照。
3. 内容变更时，刷新进程在同一个 Redis 脚本里校验本轮身份、保存版本和完整快照、设置下一轮的时间。其他进程在下次检查时只下载一次新版本。各进程先构建完整 map，然后原子切换指针。删除的记录随新版本消失，合法的空表会清空本地缓存。
4. 每次业务读取只执行一次原子指针读取和一次 map 查找。一个请求需要多次查询且要求版本一致时，先调用 `s := cache.Snapshot()`，再多次调用 `s.Get(id)`。

以 50 个进程、1 份数据集为例，健康运行时，原先每轮 50 次业务查询会降到约 1 次。默认设置下 Redis 平均承接约 25 次版本检查/秒。若一次变更生成 S 字节快照，其他 49 个实例合计约需下载 49 × S 字节。这里的读取和带宽都按数据集分别计算。

## 接入业务

需要 Go 1.26+。压缩包里的模块路径是 `example.com/tablecache`。将它解压在业务仓库旁边，可在业务模块根目录使用本地路径接入。

```bash
go mod edit -replace=example.com/tablecache=../tablecache
go get example.com/tablecache@v0.0.0
```

下面的函数接收应用已经创建的 `*sql.DB` 和 Redis 客户端。创建 go-redis 客户端时，设置 `ContextTimeoutEnabled: true`，并保留套接字读写 deadline。使用其他 `redis.Scripter` 实现时，客户端须遵守传入的 context deadline。服务启动时调用一次，传入服务整个生命周期的 context。示例使用已有的 `AdSpace` 类型和 `loadAdSpaces(db)`，两者的完整实现在 `examples/adspace/main.go`。不同表只需更换数据类型、命名空间和查询函数。

```go
func startCache(
    serviceCtx context.Context,
    db *sql.DB,
    rdb *redis.Client,
) (*tablecache.Cache[int64, AdSpace], error) {
    c, err := tablecache.New(rdb, tablecache.Options{
        Namespace: "prod:ad-space:v1",
        OnError:   func(err error) { log.Print(err) },
    }, loadAdSpaces(db))
    if err != nil {
        return nil, err
    }

    workerCtx, stop := context.WithCancel(serviceCtx)
    go func() {
        defer stop()
        if err := c.Run(workerCtx); err != nil {
            log.Print(err)
        }
    }()
    readyCtx, cancel := context.WithTimeout(serviceCtx, 25*time.Second)
    defer cancel()
    if err := c.WaitReady(readyCtx); err != nil {
        stop()
        return nil, err
    }
    return c, nil
}
```

服务就绪后，请求处理只需读本地缓存。

```go
space, ok := cache.Get(6111)
if ok && space.Enabled {
    // 使用 space.Name 等已缓存字段
}
```

示例程序使用的测试表如下。已有业务表直接替换 Loader 里的 SQL 即可。

```sql
CREATE TABLE ad_space_config (
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    enabled TINYINT(1) NOT NULL
);
INSERT INTO ad_space_config (id, name, enabled) VALUES (6111, '首页广告位', 1);
```

配置环境变量 `MYSQL_DSN` 和 `REDIS_URL` 后，在本模块根目录执行 `go run ./examples/adspace -id 6111`。示例等待快照就绪，完成一次本地查询后退出。Redis URL 默认是 `redis://localhost:6379/0`，MySQL DSN 使用 go-sql-driver/mysql 的格式。

## 配置与故障行为

| 配置 | 默认值 | 含义 |
|---|---:|---|
| Namespace | 必填 | 环境、数据集、序列化结构版本共同构成，例如 prod:ad-space:v1 |
| RefreshInterval | 1 分钟 | 上轮成功发布或确认内容未变后，允许下次回源前的等待时间 |
| PollInterval | 2 秒 | 后台 Redis 检查间隔，另加抖动 |
| LoadTimeout | 15 秒 | Loader 与编码检查的时间预算。数据库查询必须使用传入的 context |
| RedisTimeout | 2 秒 | 每次 Redis 操作的 context 超时。`New` 会检查标准 go-redis 客户端是否启用 context 超时及套接字 deadline |
| RetryInterval | 5 秒 | 回源或发布失败后，同一数据集的共享重试间隔 |
| MaxBytes | 256 MiB | 可发布、接受的 JSON 快照上限。接收方在 Redis 返回正文前检查大小，发布方先编码再检查 |
| OnError | 可选 | 后台错误回调，可接日志或监控。回调在刷新协程执行，应尽快返回 |

- **新进程启动。** Redis 已有快照时可直接就绪。Redis 为空时，一台实例回源，其他实例等待。将 `WaitReady` 接入服务启动或就绪探针，避免把“尚未加载”当成“主键不存在”。
- **MySQL 查询失败。** 已有进程保留上一版。只要 Redis 可用，新实例也能取得上一版。错误回调会收到查询、扫描、编码或大小超限等错误。Loader 要返回 `rows.Err()`，以免把半张表发布出去。
- **Redis 故障。** 已就绪实例继续本地读取。Redis 可读时，即使内存压力导致写入失败，新实例也能使用已有的完整快照。库只在取得 Redis 刷新权后发起查询。长期故障时旧数据可以持续被读取，`WaitReady` 表示曾加载完整快照。配置有时效要求的业务可监控 Loader 最近成功返回的时间。
- **刷新进程退出或暂停。** 默认租约是 LoadTimeout 加两倍 RedisTimeout，即 19 秒。实例解码共享快照后才争抢租约，加载和发布受剩余租约时间约束。到期后下一个轮询者可以接手。发布前校验随机令牌，防止失去租约的进程覆盖当前 Redis 主节点上的新结果。Redis 主从切换或丢键时可能有额外的只读查询，也可能短暂恢复到旧快照，后续成功刷新会重新收敛。
- **更新可见时间。** 默认配置下，在一轮查询刚结束后发生的变更，通常要等约 1 分钟，再加上回源、轮询和解码时间。各 ECS 逐步切换到新版本。需要同一请求内版本一致时复用同一个 Snapshot。

## 表和数据的约定

- 所有实例连接同一 Redis 主节点或同一 Redis 集群，使用相同的 Namespace、字段结构和刷新参数。Redis Cluster 下两个键共享同一哈希槽。快照键没有 TTL，协调键使用 TTL。Redis ACL 需要允许脚本执行及脚本所用的 `HGET`、`HSTRLEN`、`EXISTS`、`GET`、`HSET` 和 `PSETEX` 命令，并为快照预留空间。
- Loader 只返回完整数据集。空表返回初始化后的空 map。nil map 表示异常。单表使用一条 SELECT 即可。若一个数据集要跨多张 InnoDB 表保持一致，可由 Loader 在一个只读、一致性快照事务内完成读取。
- 键使用整数或有效 UTF-8 字符串，每个键用 JSON 编解码后须保持原值。值使用可稳定 JSON 往返的具体类型。查询结果含可空字段时自行映射。快照内的数据只读，值中含指针、map 或切片时，业务要修改须先复制。优先使用标量字段。发布进程和其他进程使用同一份序列化表示。
- 这套实现适合能够完整驻留内存、允许分钟级延迟的配置表。刷新瞬间旧 map、新 map 和编解码缓冲区可以同时存在。用真实数据量评估单机内存、一次 SELECT 的耗时和变更时的 Redis 出口带宽。默认 256 MiB 是库的大小设置，可通过 MaxBytes 调整。对于每分钟全量传输已经昂贵的大表，可改用按业务域拆分的数据集，或采用 binlog/CDC 增量分发。

## 验证

```bash
go test -race -v ./...
go test -run '^$' -bench BenchmarkGetParallel -cpu=4 -benchmem .
```

Redis 集成测试会在 127.0.0.1 临时端口启动独立的 redis-server，测试结束后关闭。运行前将 redis-server 加入 PATH。缺少它时集成用例会标记为 SKIP。

用 MySQL 8.0.46、Redis 7.4.10 和 Go 1.26.11 验证了 50 个独立进程同时冷启动。50 个进程均读到示例记录，MySQL general log 只记录了 1 次业务 SELECT。集成测试覆盖正常周期刷新、新增和删除、空表、依赖故障、租约到期、快照正文修复、超时和读取时切换版本。65,536 个整数键的并行 Get 基准记录为 0 B/op、0 allocs/op，该指标只计本地查询。
