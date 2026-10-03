# vantage6 取消/清理协议代次化改造 实施计划

## 一、仓库调研结论

### 当前取消链路
1. 用户取消：HQ `KillTask.post` → `kill_task()`（[event.py](file:///Users/ding/Documents/swe/09271/project-12/vantage6-hq/vantage6/hq/resource/event.py#L294-L336)）：
   - 向协作房间 emit `kill_containers`（`kill_list` 含 task_id/run_id/organization_id）；
   - 同步把所有未结束 run 置为 `killed by user` + `finished_at`，但**不广播任何状态事件**。
2. 节点：`NodeTaskNamespace.on_kill_containers` → `Node.kill_containers` → `ContainerManager.kill_algorithm_runs` → 列出本节点 Job，按 **run_id 字符串匹配**后 `_kill_task_job` → `delete_run_related_pods` 删除 Job/Pod/Secret；随后节点 emit `algorithm_status_change(KILLED)`。
3. 完成回调：轮询线程 `process_next_completed_run` 发现 finished Job → 读输出 → **先删资源** → `run.patch` 上报 → emit `algorithm_status_change`。

### 已确认的缺陷（与需求逐条对应）
- **无代次概念**：Job/Pod/Secret 全部以 `vantage6-run-{run_id}` 命名，仅按 run_id 匹配。节点重启/断网重连后 `sync_task_queue_with_HQ` 可能重建同一 run 的 Job（且 `__add_task_runs_to_queue` 用整数 run_id 调 `is_running`，label 实际是 `app=vantage6-run-{id}`，去重失效）；旧清理请求（以及 kill-all、重复投递的 kill 事件）会误删新一代资源。
- **终态竞争**：被 kill 的 Pod 会让 Job 出现 `failed`，轮询线程可能把它当 `CRASHED` 上报；节点对同一 run 可能既报 KILLED 又报 CRASHED/COMPLETED。
- **迟到回调复活**：HQ `on_algorithm_status_change`（[websockets.py](file:///Users/ding/Documents/swe/09271/project-12/vantage6-hq/vantage6/hq/websockets.py#L264-L341)）不检查 run 是否已终态：迟到的 `completed` 会再次广播并**释放下游依赖任务**；`run=None` 时还会抛异常。REST `run.patch` 虽拒绝改 KILLED run 的状态（仅允许 log），但节点不检查响应仍 emit socket 事件。
- **删除失败即丢失**：`delete_run_related_pods` 只 log 错误，无任何持久化重试；节点离线期间被取消的 run，Job/Secret 永久残留（取消事件是瞬态 socket emit）。
- **重启不安全**：所有状态只在内存/集群 list 中，重启后无法区分"正在执行的代次"与"待清理的旧代次"。
- **kill-all 无幂等标识**：重复/乱序到达时无法识别新旧。

### 关键约束
- 节点 `/app/tasks`（`config["task_dir"]`）是 PVC 挂载，可放跨重启的状态文件。
- 节点 RBAC 对 jobs/pods/secrets 有 create/delete/get/list/watch（无 patch）。
- HQ 不掌握节点本地代次；代次由节点分配，取消指令仍按 run_id 语义下发，节点把指令**绑定到处理时刻的当前代次**。
- 不做 HQ 数据库迁移：终态幂等只依赖现有 `finished_at/status`。
- 旧 Job 无代次 label/注解、旧 HQ 消息无 event_id/issued_at，必须平滑兼容。

## 二、设计概要

### 1. Run 代次（attempt / generation，节点侧）
- 节点为每次执行 attempt 分配单调递增 `attempt_seq`（持久化），tag 为 `g{seq}` 的 base36 短串。
- 资源命名与标签：
  - Job：`vantage6-run-{run_id}-{tag}`；新增 label `run_id`、`attempt_tag`（保留 `node_id`）；注解新增 `attempt_tag`、`secret_name`、`docker_login_secret_name`。
  - Pod template 同步加 `run_id`、`attempt_tag`，`app=<job名>`。
  - 主 Secret 名 = Job 名；docker login Secret = `docker-login-secret-run-id-{run_id}-{tag}`。
  - 输入/输出 hostPath 文件放到 `/app/tasks/{run_id}/{tag}/` 目录，按代次隔离；session 目录共享不变。
- 旧资源（无 tag）按 legacy 处理：tag=None、沿用原名、删除时回退旧 Secret 名。

### 2. 持久化 RunRegistry（新文件）
路径 `{task_dir}[/{dev_extension}]/.v6_node_state.json`，临时文件 + `os.replace` 原子写，带线程锁，结构：
- `counter`：attempt 自增序号；
- `active`：run_id → {tag, job_name, state: running|killing, created_at}；
- `cleanups`：清理墓碑列表，每条 = {run_id, tag, job_name, secret 名, reason(killed/completed/shutdown/reconcile/legacy), created_at, retries, last_error, logs, logs_delivered}，**精确钉死代次资源名**；
- `events`：已处理 kill 事件 id → 时间戳（GC 24h）；
- `last_killall_ts`：最近 kill-all 时间，拒绝更旧的乱序 kill-all。

### 3. 取消与清理协议（节点）
- 收到 kill：collaboration/node 过滤后，先做 event_id 去重与 kill-all 时间判旧；对每个 run_id：
  - `active` 命中 → 先置 `killing`（先写墓碑再删 K8s，write-ahead），精确删除该代次 Job/Pod/Secret，**KILLED 终态仅上报一次**（墓碑/registry 去重）。
  - active 未命中但集群存在同 `run_id` label 的 Job（registry 丢失/legacy）→ 从 Job label 读取代次后补建墓碑再删。
  - 无资源 → 不产生 KilledRun（HQ 早已置 KILLED）。
  - kill-all（无 kill_list）：对当前 active 全部执行上述流程；老消息无 event_id/issued_at 时按旧语义处理。
- 后台 reconcile 线程（~10s）：重试墓碑删除；404 视为成功；成功后补发 kill log（仅 log 的 PATCH，断网则继续重试），全部成功后移除墓碑。
- 轮询线程：发现 finished Job 时先查 registry，若该代次处于 killing/已有墓碑 → **不报 CRASHED/COMPLETED**；正常完成也改为先建 reason=completed 墓碑再删（正常完成清理同样可重试）。
- 启动 reconcile：registry 与集群 Job 对账（重建 active、补/销墓碑）。
- 与 HQ 对账（挂在 `on_sync`，断网重连即触发）：取 open runs 集合；本地存在 Job 但其 run_id 不在 open 集合（HQ 已终态，含离线期间被取消）且无墓碑 → 建 reason=reconcile 墓碑清理。
- 启动新代次前置校验：出队后用 `run.list(state=open, task_id)` 确认 run 仍 open，否则丢弃，杜绝取消后重建。
- 节点关停 `cleanup()`：active 全部置 killing + 墓碑（reason=shutdown，initiator=NODE_SHUTDOWN），尽力删除，未尽删除由下次启动 reconcile 完成。

### 4. HQ 终态幂等
- `on_algorithm_status_change`：run 不存在 → 记录并忽略；run 已有 `finished_at` → 任何迟到/重复状态（completed/crashed/active/killed）一律忽略，不广播、不释放依赖任务。
- 终态广播权收敛：
  - `run.patch` 正常完成持久化后，由 HQ 广播 `algorithm_status_change`（已有）并补发依赖任务的 `new_task_update`（逻辑从 socket handler 迁入，保证两种事件到达顺序都正确）。
  - `kill_task()` 置 KILLED 后，由 HQ 逐 run 广播 `status_update` + `algorithm_status_change(KILLED)`，节点 ack 变为可丢弃重复；节点离线也不影响 UI 终态。
- 节点 `__report_finished_run`：PATCH 响应含 `msg`（被 HQ 拒绝，如已 KILLED）时不再 emit 状态事件。
- 取消事件新增可选 `event_id`(uuid) 与 `issued_at`（ISO），节点端去重/判旧；旧消息无字段照常处理。

### 5. 兼容性
- 旧节点 + 新 HQ：旧节点仍发 KILLED ack（在 HQ 侧成为重复事件被忽略），HQ 已自行广播 KILLED；REST 流程不变。
- 新节点 + 旧 HQ：节点仍发 KILLED ack（保留发送）；旧 HQ 无 event_id 字段时按当前代次处理。
- 旧 Job（无 tag/注解）：legacy 回退命名删除；CLI `v6 node stop` 与 dev 清理脚本改为按 Job 实际名/注解删除并回退旧名。
- 单节点普通任务：链路不变，仅 Job 名多代次后缀（内部标识，无外部依赖）。

## 三、改动文件

### 节点 `vantage6-node/`
- `vantage6/node/k8s/run_registry.py`（新）：`RunRegistry` 持久化状态、attempt 分配、墓碑/事件去重、原子写与损坏恢复。
- `vantage6/node/k8s/data_classes.py`：`ToBeKilled`/`KilledRun` 增加可选 `attempt_tag`；新增 `CleanupRecord` 等结构（如采用 dataclass）。
- `vantage6/node/k8s/run_io.py`：`attempt_tag` 参数；`container_name`、IO 文件目录按代次；`from_dict` 读注解。
- `vantage6/node/k8s/container_manager.py`：代次化命名/标签/注解、registry 接入、kill 绑定代次与墓碑、完成轮询守卫、reconcile 线程与启动/HQ 对账、`is_run_active(run_id)`、关停清理。
- `vantage6/node/__init__.py`：出队 open 前置校验、PATCH 拒绝后不发事件、sync 触发对账、关停路径。
- `vantage6/node/socket.py`：仅透传（kill_info 原样进入 Node.kill_containers，逻辑在 Node 层）。
- `tests_node/`：新增 `test_run_registry.py`；扩充 `test_container_manager.py`（代次匹配、墓碑防误删、完成/kill 竞争、legacy 回退）。

### CLI/公共删除函数 `vantage6/`
- `vantage6/cli/node/common/task_cleanup.py`：删除入参改为显式资源名/label 选择器，返回逐项成功结果（404=成功），保留旧签名兼容包装。
- `vantage6/cli/node/stop.py`：按列出的 Job 实际名 + 注解（回退旧规则）清理。
- `tests_cli/`：新增 `test_task_cleanup.py`（mock API 验证代次名、404、部分失败）。

### HQ `vantage6-hq/`
- `vantage6/hq/resource/event.py`：emit 增加 `event_id`/`issued_at`；`kill_task` 终态后广播 `status_update` 与 `algorithm_status_change(KILLED)`。
- `vantage6/hq/websockets.py`：终态/空 run 守卫，移除终态后的广播与依赖释放。
- `vantage6/hq/resource/run.py`：PATCH 完成分支补发依赖任务 `new_task_update`。
- `tests_hq/`：`test_task.py` 扩充 kill 广播断言；新增 websocket 迟到回调用例（KILLED 后 completed 被忽略、依赖任务不释放、run 缺失不异常）。

## 四、实施步骤（依赖顺序）
1. 节点数据层：`run_registry.py` + data_classes 扩展 + 单测。
2. RunIO 代次化（命名/IO 目录/注解）。
3. CLI 公共删除函数改造（显式资源名 + 结果返回）及 stop.py 适配。
4. ContainerManager：代次标签/命名、registry 接入、kill 墓碑化、完成守卫、reconcile 线程、启动与 HQ 对账。
5. Node 编排层：open 前置校验、上报响应处理、sync 对账、关停。
6. HQ：event.py 事件 id 与终态广播；websockets 守卫；run.patch 依赖释放迁移。
7. 补齐/更新全部测试；`ruff format .` + `ruff check .`。
8. 运行 `python utest.py --node --hq --cli --common` 全绿。

## 五、验证
- 单测：registry 持久化/损坏恢复/墓碑钉代次；manager 两代并存时旧墓碑只删旧代次；kill 与完成竞争只出一个 KILLED；legacy Job/消息回退；HQ 迟到 completed 不复活、不释放依赖；REST PATCH 400 时节点不广播。
- 静态：ruff 格式与 lint。
- 手动推演（评审说明，不执行）：取消→立即重试→断网→重连→operator 重启五个时序下资源与终态正确。

## 六、风险与对策
- **同一 run_id 的 hostPath 并发**：IO 文件按代次子目录隔离，session 目录本就设计为共享。
- **Job 名长度**：`vantage6-run-{10位数字}-g{短base36}` ≈ 35 字符，符合 DNS-1123（≤63）。
- **新老节点/HQ 混部**：所有新增字段可选，旧资源/旧消息走 legacy 回退；KILLED 广播双保险（HQ + 节点 ack）。
- **误删仍在跑的 Job**：对账删除的前提是 HQ 已置终态（open 集合不含）；墓碑只钉死显式资源名，重试不可能触及新代次。
- **状态文件损坏**：加载失败时备份损坏文件并从集群 Job 注解重建 active/墓碑。
- **正常完成结果丢失的既有行为**（先删 Job 后 PATCH）：不在本次范围，完成路径墓碑化后仅保证删除可重试，不改变上报时序。
