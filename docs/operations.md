# 部署与运维

## 健康检查

- API 存活：`GET /health/live`
- API 就绪：`GET /health/ready`
- PostgreSQL：`pg_isready`
- Redis：`redis-cli ping`
- MinIO：`mc ready local`
- ClamAV：`clamdcheck.sh`

## 关键监控

- API 错误率、p50/p95/p99 延迟。
- PostgreSQL 连接数、慢查询和磁盘使用率。
- Redis 内存、BullMQ 等待任务和失败任务。
- `media_assets` 中 `processing` 或 `failed` 数量。
- `manual_review` 媒体队列长度。
- `pending` 内容与评论队列长度。
- outbox `pending`、`failed` 数量。
- `delete_after <= now()` 的原图数量。
- 公开桶中是否存在未被数据库引用的对象。
- 台账 `media_object_ledger` 中 `delete_pending`/`superseded` 到期对象的积压量。
- `media_processing_attempts` 中运行超过 20 分钟仍为 `running` 的尝试数（正常应为 0，由维护任务自动恢复）。
- 对账游标 `media_reconcile_cursors` 是否长期停在同一位置（对象存储列举可能异常）。

### 追溯与处理孤儿对象

- 按对象反查归属：`SELECT * FROM media_object_ledger WHERE bucket = $1 AND object_key = $2;`，再结合 `media_processing_attempts` 查看该媒体的每次处理触发者、worker、错误与时间线。
- 按媒体追溯全部对象历史：`SELECT object_role, state, attempt_id, origin, note, written_at, deleted_at FROM media_object_ledger WHERE media_id = $1 ORDER BY written_at;`。
- 对账任务会自动重建处于稳定终态媒体的丢失引用；宽限期内的私有桶孤儿不会立即删除，确认无主后可等待到期，或由运维直接将台账行改为更早的 `delete_after`。
- 严禁手动在桶中删除对象而不更新台账；确需手工处理时，删除后在台账补一条 `deleted` 状态记录，保证下轮对账不产生噪声。

## 备份

- PostgreSQL 每日全量备份并保留 WAL 或等价连续归档。
- MinIO 启用版本化和跨盘/跨区域容灾时，分别备份隔离桶和公开桶。
- `.env.production` 和密钥应保存在密钥管理系统，不进入镜像或仓库。
- 每季度执行一次恢复演练，验证数据库、公开媒体和迁移记录。

## 发布

1. 构建并锁定 API、Worker、Web 镜像。
2. 备份数据库。
3. 执行一次 `migrate` 容器。
4. 启动新 API 和 Worker。
5. 验证就绪检查、登录、地图查询和媒体处理。
6. 再切换 Caddy 流量。
7. 保留上一版本镜像用于回滚。

## 隐私事件

发现未模糊媒体或原图泄露时：

1. 立即停止相关媒体发布并删除公开对象。
2. 暂停媒体 Worker，防止继续复制到公开桶。
3. 根据对象访问日志确认影响范围。
4. 修复处理管线并执行全量扫描。
5. 删除或隔离受影响对象。
6. 记录事故、根因、修复和回归测试。
7. 按法律与运营要求通知用户。

## 数据保留

- 成功处理原图：24 小时。
- 失败处理原图：最多 7 天。
- 邮箱验证令牌：24 小时。
- 密码重置令牌：30 分钟。
- 过期刷新令牌：30 天清理。
- 账号删除冷静期：30 天。
- 审计与审核记录：默认 180 天，生产可按法务要求延长。
