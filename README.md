# 🚀 企业级迭代与项目管理平台

https://img.shields.io/badge/Go-1.24+-00ADD8?logo=go&logoColor=white

https://img.shields.io/badge/Architecture-Microservices-blueviolet

https://img.shields.io/badge/RPC-gRPC-4169E1?logo=grpc&logoColor=white

> **基于 Go 语言构建的下一代企业级迭代管理与协作平台**
>
> 采用 API Gateway + gRPC 微服务架构，深度融合 DevOps 与 AI 智能辅助，为企业提供高效、稳定、智能的项目全生命周期管理能力。

------

## 🏗️ 架构设计

项目采用 **三层微服务架构**，通过 API Gateway 统一收敛流量，内部服务间使用高性能 gRPC 通信，实现高内聚低耦合。

```text
┌───────────────────────────────────────────────────────────┐
│                     Client (Web / App)                    │
└───────────────────────┬───────────────────────────────────┘
                        │ HTTP/HTTPS (Port: 8080)
┌───────────────────────▼───────────────────────────────────┐
│                    API Gateway (Gin)                       │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────┐ │
│  │ Middlewares │  │ Auth (JWT)  │  │ Routing & Limiting│ │
│  └─────────────┘  └─────────────┘  └───────────────────┘ │
└───────────────────────┬───────────────────────────────────┘
                        │ gRPC (Internal Network)
        ┌───────────────┼────────────────┐
        │               │                │
┌───────▼────────┐ ┌────▼─────────────┐ ┌─▼─────────────────┐
│  项目服务       │ │    用户服务       │ │   (其他微服务...)  │
│ (toProject/)   │ │   (toUser/)      │ │                     │
│ Port: 50051    │ │  Port: 50052     │ │                     │
└────────────────┘ └──────────────────┘ └─────────────────────┘
```

### 🧩 核心服务概览

| 服务组件            | 端口    | 核心职责                                  |
| ------------------- | ------- | ----------------------------------------- |
| **API Gateway**     | `8080`  | HTTP 入口、路由分发、JWT 鉴权、限流、日志 |
| **Project Service** | `50051` | 项目/任务/迭代核心逻辑、权限、数据分析    |
| **User Service**    | `50052` | 用户认证、登录注册、组织机构、RBAC        |

------

## 🛠️ 技术栈

<details open>

<summary>**点击展开/收起技术全景**</summary>

| 领域           | 技术选型                                                |
| -------------- | ------------------------------------------------------- |
| **核心语言**   | Go 1.24+                                                |
| **通信协议**   | HTTP/REST (Gin) + gRPC + Protocol Buffers               |
| **数据持久化** | MySQL 8.0 (GORM) + Redis (Cache/Lock)                   |
| **消息队列**   | Apache Kafka                                            |
| **对象存储**   | MinIO                                                   |
| **服务治理**   | Etcd (Service Discovery) + Nacos (Config Center)        |
| **可观测性**   | Jaeger (Tracing) + Prometheus (Metrics) + ELK (Logging) |
| **安全认证**   | RSA 非对称加密 JWT + Gopher-Lua (动态权限)              |
| **AI 集成**    | 通义千问 (DashScope)                                    |

</details>

------

## 📂 项目结构

采用清晰的模块化布局，区分 API 层、业务服务层与公共组件。

```text
demo1/
├── api/                          # API Gateway (HTTP 入口)
│   ├── api/
│   │   ├── midd/                # 中间件 (Auth/CORS/RateLimit)
│   │   ├── project/             # 项目/任务/分析/系统 API Handler
│   │   │   ├── project.go
│   │   │   ├── task.go
│   │   │   ├── analysis.go
│   │   │   └── router.go
│   │   └── user/                # 用户相关 Handler
│   ├── config/                   # 网关配置
│   └── main.go
│
├── toProject/                    # 项目核心微服务 (gRPC)
│   ├── api/proto/               # Proto 定义 (项目/任务/权限/分析)
│   ├── internal/
│   │   ├── domain/              # 领域逻辑
│   │   ├── repo/                # 仓储层 (DB Access)
│   │   └── database/            # DB 连接与事务
│   ├── pkg/service/             # gRPC Service 实现
│   └── main.go
│
├── toUser/                       # 用户微服务 (gRPC)
│   ├── api/proto/               # Login Service Proto
│   └── main.go
│
├── others/                       # 公共基础库
│   ├── jwts/                    # RSA JWT 工具
│   ├── kk/                      # Kafka 封装
│   ├── min/                     # MinIO 客户端
│   ├── llm/                     # 通义千问 LLM 集成
│   └── discovery/               # Etcd/Nacos 服务发现
│
├── docker-compose.yaml           # 基础设施编排
└── README.md
```

------

## ✨ 核心功能

### 📊 敏捷项目管理

- **迭代管理**：创建、编辑、归档、回收站恢复、收藏及模板化创建。
- **任务协作**：支持任务阶段（Stage）、子任务、优先级、工时记录、评论 @提及。
- **进度追踪**：自动计算项目进度，提供树形结构导航。

### 👥 团队与权限

- **RBAC 权限体系**：细粒度的项目/组织权限控制。
- **动态菜单**：基于 **Gopher-Lua** 引擎实现按钮级权限过滤，支持运营策略热更新。
- **审批流**：内置权限申请与审批工作流。

### 📈 数据分析与 AI 赋能

- **可视化报表**：交付率分析、团队负载统计。
- **敏捷图表**：实时燃尽图展示。
- **AI 智能助手**：集成通义千问，基于历史数据实现 **延期风险识别** 与 **优先级智能推荐**。

### 📁 文件与存储

- 集成 MinIO 对象存储。
- 支持任务/项目文件关联、断点续传与大文件分片上传。

------

## 🚀 快速开始

### 前置条件

- Go 1.24+
- Docker & Docker Compose
- Make (可选)

### 1. 启动基础设施

一键拉起 MySQL, Redis, Etcd, Nacos, Jaeger, Kafka, MinIO。

```bash
docker-compose up -d
```

### 2. 配置中心

在 **Nacos** 中创建配置文件 `config.yaml`，配置数据库连接串、Redis 地址、Kafka Broker 等信息。

### 3. 编译 Proto 文件

```bash
# 进入项目服务 proto 目录
cd toProject/api/proto

# 执行生成脚本
sh gen.bat  # Windows
# 或
protoc --go_out=. --go-grpc_out=. *.proto  # macOS/Linux
```

### 4. 启动服务

建议在不同终端窗口启动，或使用 tmux/screen。

```bash
# 终端 1: 启动用户服务
cd toUser && go run main.go

# 终端 2: 启动项目服务
cd toProject && go run main.go

# 终端 3: 启动 API 网关
cd api && go run main.go
```

### 5. 验证

访问 API 网关：`http://localhost:8080`

------

## 🔌 API 路由概览

所有 API 均通过 API Gateway 转发，Base Path 通常为 `/iteration`。

<details>

<summary>**点击查看详细 API 列表**</summary>

| 分类     | Method | Endpoint                        | 描述          |
| -------- | ------ | ------------------------------- | ------------- |
| **项目** | `POST` | `/selfList`                     | 我的迭代列表  |
|          | `POST` | `/save`                         | 创建/更新迭代 |
|          | `POST` | `/read`                         | 迭代详情      |
|          | `POST` | `/recycle`                      | 软删除        |
|          | `POST` | `/recovery`                     | 恢复迭代      |
| **任务** | `POST` | `/stages`                       | 任务阶段列表  |
|          | `POST` | `/stages/list`                  | 需求列表      |
|          | `POST` | `/save`                         | 新建/编辑需求 |
|          | `POST` | `/sort`                         | 需求排序      |
| **团队** | `POST` | `/account`                      | 账户列表      |
|          | `POST` | `/team/save`                    | 创建/编辑团队 |
| **分析** | `POST` | `/analysis/interation/delivery` | 迭代交付率    |
|          | `POST` | `/analysis/burndown`            | 燃尽图        |
|          | `POST` | `/analysis/team/load`           | 团队负载      |
|          | `POST` | `/analysis/ai/recommend`        | AI 智能分析   |
| **权限** | `POST` | `/auth`                         | 权限列表      |
|          | `POST` | `/auth/apply`                   | 权限申请      |
| **文件** | `POST` | `/file/upload`                  | 上传文件      |
|          | `POST` | `/file/list`                    | 文件列表      |

</details>

------

## 🛡️ 中间件与治理

| 中间件            | 功能描述                          |
| ----------------- | --------------------------------- |
| **JWT Auth**      | 基于 RSA 非对称加密的无状态认证   |
| **Rate Limit**    | 接口限流，防止恶意攻击            |
| **CORS**          | 跨域资源共享支持                  |
| **Recovery**      | 全局异常捕获与兜底                |
| **Prometheus**    | 暴露 `/metrics`端点，采集性能指标 |
| **OpenTelemetry** | 分布式链路追踪 (Jaeger)           |
| **PProf**         | 性能剖析接口 (`/debug/pprof`)     |

------

## 🚦 缓存与性能

- **多级缓存**：本地 LRU Cache + Redis 分布式缓存，降低数据库压力。
- **缓存一致性**：Cache-Aside Pattern，保证数据最终一致。
- **防击穿**：使用 RedSync 实现分布式锁，解决热点 Key 重建时的并发穿透问题。
- **性能优化**：结合 `pprof`深度剖析 CPU/内存瓶颈，针对性优化核心链路。

------

## 🔭 可观测性

- **分布式追踪**：Jaeger 全链路追踪，可视化请求拓扑与耗时。
- **指标监控**：Prometheus 采集 QPS、Latency、Error Rate 等关键指标。
- **日志分析**：ELK (Elasticsearch, Logstash, Kibana) 集中式日志管理与检索。
