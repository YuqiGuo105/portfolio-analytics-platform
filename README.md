# portfolio-analytics-platform

访客分析 + 地理聚合 + 告警流水线的多模块 Maven 项目，作为 [`portfolio-ai-platform`](https://github.com/YuqiGuo105/portfolio-ai-platform) 与 [`portfolio-notification-service`](https://github.com/YuqiGuo105/portfolio-notification-service) 的姊妹仓。

> **设计基线（cheap plan）**：Aiven Kafka Hobbyist（每 topic 2 分区）+ Aiven Valkey（dedup 与查询缓存）+ Supabase Postgres（rollups 与 alert 元数据）。**不引入 ClickHouse、Kafka Streams、S3/Iceberg**——所有聚合用 Postgres 的两档 rollup（5m / 1d）。
>
> 这是为了把整个分析链路压在已经在线的免费/低价基础设施上，部署目标是已有的 Cloud Run + Aiven + Supabase。

---

## 模块

| Module | 端口 | 职责 |
|---|---|---|
| `analytics-common` | — | `RawEvent` / `EnrichedEvent` DTO、`GeoLevel` / `Granularity` enum、Kafka topic 常量、`DlqProducer` 复用类、`Outcome` enum |
| `analytics-enrichment-service` | 8092 | 消费 `analytics.raw.events`；GeoIP + UA 解析 + bot 评分 + `ip_hash = HMAC(salt, ip)` 后丢弃原始 IP + snap 到 METRO；写 `analytics.enriched.events`，bad message 进 DLQ |
| `analytics-aggregator-service` | 8093 | 消费 `analytics.enriched.events`；按 (site_id, bucket_time, granularity, geo_level, geo_area_id, event_type, dims) UPSERT 到 `geo_time_rollups`；5m 与 1d 两档，1d 由 5m 级联派生 |
| `analytics-alerts-service` | 8094 | `alert_rules` 与 `incidents` 的 CRUD + 每分钟 `@Scheduled` 评估器；命中后 HTTP POST 到 `portfolio-notification-service /api/content-events`（携带 `X-Internal-Token`） |

所有 Spring Boot 模块都用 manual ack + DONE/DLQ/RETRY 三态，完全复刻 `portfolio-notification-service` 的 `ContentEventConsumer`。

---

## 隐私不变量（在代码里硬编码，不靠运行时配置）

1. **永不持久化原始 IP**：`RawEvent.ipRaw` 仅在 enrichment 内存中存活；`EnrichedEvent` 类型本身没有 `ipRaw` 字段，只暴露 `ipHash`。
2. **METRO 是最细的位置粒度**：`EnrichedEvent.geoLevel ∈ {GLOBAL, COUNTRY, REGION, METRO}`，没有 city/lat/lng 字段。
3. **地图坐标只能来自 `geo_areas.center_lat/lng`**，永不返回访客点。
4. **Postgres 不在 ingestion 热路径**：Ingestion API（在 `Portfolio` 仓）只做 `kafka.produce` + 返回 202；Postgres 只承担 rollup、alert 规则、incident 元数据。

---

## 本地开发

```bash
cp .env.example .env
# 填入 Aiven Kafka / Valkey / Postgres 的真实值（绝不要把 .env 提交！）

# 构建 + 60% line coverage 卡口
./mvnw -B verify -Pcoverage

# 起本地容器
docker compose up --build
```

每个 service 暴露 `/actuator/health`。

---

## 与既有仓库的契约

* **生产者**：[`Portfolio`](https://github.com/YuqiGuo105/Portfolio) 仓里的 `pages/api/analytics/ingest.js`（待加）produce 到 `analytics.raw.events`。旧的 `/api/track` 与 `/api/click` 保留，但 README 标记 deprecated。
* **告警下游**：本仓的 `analytics-alerts-service` 不直接发邮件，而是 HTTP POST `portfolio-notification-service`：
  ```
  POST /api/content-events
  Headers: X-Internal-Token: $NOTIFICATION_INTERNAL_TOKEN
  Body:    { eventType, topic, title, summary, idempotencyKey, ... }
  ```
  完全复用现有的订阅扇出 + 重试 + EMAIL/WEB channel 切分逻辑。

---

## 部署目标

每个 Spring Boot 模块各自构建一个 Docker 镜像（`Dockerfile` 沿用 notification-service 的 `maven:3.9.9-eclipse-temurin-21 → eclipse-temurin:21-jre-jammy` 两阶段构建 + 非 root 用户），部署到 **Cloud Run**。配置走 Cloud Run secret bindings，与 `portfolio-notification-service` 同款。

---

## License

私有项目，仅 Yuqi Guo 个人 portfolio 使用。
