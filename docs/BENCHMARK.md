# AgentMesh AIOps — 并发压测与可靠性基准报告（Phase 2.1）

本文档记录在本地真实 **PostgreSQL 16.16**（Docker `postgres:16-alpine`）与 **Redis 7**（Docker `redis:7-alpine`）环境下，对 AgentMesh 多智能体编排与流式归档闭环进行并发压测的实测数据。

---

## 1. 压测配置与隔离策略

- **压测脚本**：`backend/benchmarks/run_load_benchmark.py`
- **执行命令**：
  ```bash
  cd backend
  .\.venv\Scripts\python.exe benchmarks\run_load_benchmark.py --tiers 10 30 50 --workers 4 --archivers 2
  ```
- **模型策略**：强制锁定 `LLM_PROVIDER=mock`（0 外部 Token 消耗，聚焦平台调度、数据库锁竞争与流式归档可靠性）。
- **数据库与缓存隔离**：运行在独立测试库 `aiops_mesh_test`（`127.0.0.1:5436`）与 Redis DB `1`（`127.0.0.1:6386`），每档压测前清空表与 Redis DB，避免污染开发环境数据。
- **并发拓扑**：
  - **4 个并发 `AgentWorker`**：通过 PostgreSQL 16 `SELECT ... FOR UPDATE SKIP LOCKED` 并发抢占 `agent_tasks` 队列。
  - **2 个并发 `TrajectoryStreamConsumer`**：加入 Redis Streams 消费者组 `cg:trajectory-archiver`，并发消费 `stream:aiops:events`、写入 PG16 `incident_trajectories` 并在事务提交后执行 `XACK`。
- **单次请求完整闭环（每条故障单包含 5 个异步 Agent 任务 + 13 条 Trace 事件）**：
  `注入混合故障（PG 连接池耗尽 / 慢 SQL 锁争用 / Redis 缓存击穿）` → `COORDINATOR` → `LOG_DIAGNOSTICS + DB_DIAGNOSTICS` → `REMEDIATION`（挂起在 `PENDING_APPROVAL`）→ `自动审批放行` → `EXECUTOR` → `Redis Streams 消费归档 Trajectory` → `RESOLVED`。

---

## 2. 实测基准数据（本地真实 PG16 + Redis 7）

| 并发度 | 总请求数（故障单） | 总执行任务数（AgentTask） | 成功率 | 平均端到端延迟 (ms) | P95 延迟 (ms) | P99 延迟 (ms) | 故障单吞吐量 (req/s) | 任务吞吐量 (task/s) | 重复任务执行次数 | 压测后 Redis `XPENDING` |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10** | 10 | 50 | **100.00%** | 1,769.83 | 1,953.89 | 1,953.89 | 5.02 | 25.09 | **0** | **0** |
| **30** | 30 | 150 | **100.00%** | 8,599.00 | 9,176.27 | 9,237.87 | 3.22 | 16.08 | **0** | **0** |
| **50** | 50 | 250 | **100.00%** | 19,938.53 | 21,108.72 | 21,158.80 | 2.35 | 11.75 | **0** | **0** |

---

## 3. 可靠性硬性断言验证

在 10 / 30 / 50 三档并发下，脚本均自动校验并通过以下强一致断言：

1. **零失败率（`failed == 0`）**：90 条端到端故障单（累计 450 个异步 `AgentTask`）全部完成 `RESOLVED` 状态流转与 Trajectory 归档。
2. **零重复执行（`duplicate_task_executions == 0`）**：
   - `agent_tasks.attempts > 1` 计数为 **0**，证明 4 个并发 Worker 在 `FOR UPDATE SKIP LOCKED` 下无任何重复抢占。
   - 同一故障单下不存在重复角色任务（`GROUP BY incident_id, agent_role HAVING count(*) > 1` 为 **0**），证明 `LOG_DIAGNOSTICS` 与 `DB_DIAGNOSTICS` 并行扇入汇聚时，通过 `Incident` 行锁准确控制 `REMEDIATION` 仅入队一次。
3. **零未确认消息堆积（`XPENDING == 0`）**：每档压测结束时，Redis Streams `stream:aiops:events` 的 `cg:trajectory-archiver` 消费者组 Pending Entries List 全部清零，所有事件均在 PG16 提交后完成 `XACK`。

---

## 4. 瓶颈观测与后续优化方向

- 本次压测中，单条故障单需经历约 9 次独立 PG16 事务提交（含默认 `synchronous_commit=on` 的 Docker Desktop 磁盘 fsync），且压测客户端以 10ms 轮询状态，导致并发度从 10 升至 50 时，受限于本地单机 PG16 事务提交 IOPS 与 4 Worker 固定并发，端到端延迟线性上升。
- **后续优化方向（Phase 2 后续迭代）**：
  1. 将压测客户端的状态等待由轮询改为基于 Redis Streams / SSE 事件驱动，减少读查询压力；
  2. 为 `AgentWorker` 引入可配置的批量拉取或水平扩缩容策略（如 8~16 Worker），并结合 Prometheus 指标观测 PG 连接池等待耗时与 `SKIP LOCKED` 队列深度。
