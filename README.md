# iteration 企业级迭代/项目管理平台

基于 Go 语言构建的企业级迭代与项目管理平台，采用微服务架构设计，提供完整的项目管理、任务协作、团队管理、权限控制以及数据分析和 AI 智能辅助功能。

## 架构设计

项目采用 **API Gateway + gRPC 微服务** 的三层架构：

```
┌─────────────────────────────────────────────────────────┐
│                     API Gateway (api/)                   │
│                    Gin HTTP + 中间件                      │
└─────────────────────────┬───────────────────────────────┘
                          │ gRPC
          ┌───────────────┴───────────────┐
          │                               │
┌─────────▼─────────┐           ┌────────▼────────┐
│   项目服务          │           │    用户服务       │
│  (toProject/)     │           │   (toUser/)      │
│ gRPC + 业务逻辑    │           │  gRPC + 认证     │
└───────────────────┘           └─────────────────┘
```

### 核心服务

| 服务         | 端口  | 职责                                 |
| ------------ | ----- | ------------------------------------ |
| **API 网关** | 8080  | HTTP API 入口、路由分发、中间件处理  |
| **项目服务** | 50051 | 项目、任务、权限、团队等核心业务逻辑 |
| **用户服务** | 50052 | 用户认证、登录注册、组织机构管理     |

## 技术栈

| 类别      | 技术                   |
| --------- | ---------------------- |
| 语言      | Go 1.24+               |
| HTTP 框架 | Gin                    |
| RPC 框架  | gRPC + Protobuf        |
| 数据库    | MySQL 8.0 + GORM       |
| 缓存      | Redis                  |
| 消息队列  | Kafka                  |
| 对象存储  | MinIO                  |
| 服务发现  | Etcd                   |
| 配置中心  | Nacos                  |
| 链路追踪  | Jaeger + OpenTelemetry |
| 监控      | Prometheus             |
| 日志      | ELK                    |

## 项目结构

```
demo1/
├── api/                          # API 网关服务
│   ├── api/
│   │   ├── midd/                # 中间件（认证、CORS、限流、日志）
│   │   ├── project/             # 项目相关 API Handler
│   │   │   ├── project.go       # 项目管理
│   │   │   ├── task.go          # 任务管理
│   │   │   ├── analysis.go       # 数据分析
│   │   │   ├── account.go       # 账户管理
│   │   │   ├── department.go     # 部门管理
│   │   │   ├── auth.go           # 权限管理
│   │   │   ├── menu.go           # 菜单管理
│   │   │   ├── file.go           # 文件管理
│   │   │   ├── system.go         # 系统管理
│   │   │   └── router.go         # 路由注册
│   │   └── user/                # 用户相关
│   ├── config/                   # 配置管理
│   ├── pkg/model/               # API 层数据模型
│   └── main.go                  # 入口
│
├── toProject/                    # 项目微服务
│   ├── api/proto/               # Protocol Buffer 定义
│   │   ├── project_service.proto
│   │   ├── task_service.proto
│   │   ├── auth_service.proto
│   │   ├── analysis.proto
│   │   ├── account_service.proto
│   │   ├── department_service.proto
│   │   ├── menu_service.proto
│   │   ├── file.proto
│   │   └── system.proto
│   ├── internal/
│   │   ├── dao/                 # 数据访问对象
│   │   ├── data/                # 数据模型/实体
│   │   ├── domain/              # 领域逻辑
│   │   ├── repo/                # 仓储层
│   │   ├── rpc/                 # RPC 客户端
│   │   └── database/            # 数据库连接/事务
│   ├── pkg/service/             # gRPC 服务实现
│   └── main.go
│
├── toUser/                       # 用户微服务
│   ├── api/proto/               # Protocol Buffer 定义
│   │   └── login_service.proto
│   ├── internal/
│   │   ├── dao/
│   │   ├── data/
│   │   └── repo/
│   ├── pkg/service/             # gRPC 服务实现
│   └── main.go
│
├── rpcOrgrpc/                    # 共享的 Protobuf 生成代码
├── others/                       # 公共工具库
│   ├── jwts/                    # JWT 工具（RSA）
│   ├── kk/                      # Kafka 生产者/消费者
│   ├── llm/                     # 通义千问 API 集成
│   ├── min/                     # MinIO 对象存储
│   ├── encrypts/                # 加密工具（AES）
│   ├── errs/                    # 错误定义
│   ├── elk/                     # ELK 日志
│   ├── logs/                    # 日志工具
│   └── discovery/               # 服务发现
│
├── docker-compose.yaml           # 容器编排
└── README.md
```

## 核心功能

### 1. 项目/迭代管理

- 创建、编辑、软删除、恢复项目
- 项目归档与回收站管理
- 项目收藏功能
- 项目模板系统（支持预设任务阶段）
- 项目进度自动更新
- 项目访问控制（开放/私有/自定义白名单）
- 项目树形结构导航

### 2. 任务/需求管理

- 任务阶段（Stage）管理
- 任务的创建、编辑、排序、软删除
- 任务负责人分配
- 任务优先级和状态追踪
- 任务关联资源（文件、需求链接）
- 子任务支持
- 任务工时记录与统计
- 任务评论与@提及功能
- 完整的任务操作日志

### 3. 团队与权限管理

- 成员列表与项目管理权限
- 多级权限体系（项目权限/组织权限）
- 权限申请与审批工作流
- 动态菜单权限控制（支持 Gopher-Lua 脚本过滤）
- 部门/团队组织架构管理

### 4. 数据分析

| 分析项      | 说明                                         |
| ----------- | -------------------------------------------- |
| 交付率分析  | 统计项目的完成率、进行中、逾期数量           |
| 燃尽图      | 展示项目剩余工作量随时间的变化趋势           |
| 团队负载    | 展示团队成员任务数量、工作时长、负载等级     |
| AI 智能推荐 | 集成阿里云通义千问，提供风险识别、优先级建议 |

### 5. 文件管理

- 集成 MinIO 对象存储
- 支持文件上传、下载、列表查询
- 任务文件与项目文件关联

## 环境要求

- Go 1.24+
- MySQL 8.0+
- Redis 6.0+
- Kafka
- Etcd
- Nacos
- MinIO
- Jaeger

## 快速开始

### 1. 启动基础设施

```bash
docker-compose up -d
```

这将启动：MySQL、Redis、Etcd、Nacos、Jaeger

### 2. 配置 Nacos

在 Nacos 中创建配置文件 `config.yaml`，包含数据库、Redis、Kafka 等配置。

### 3. 编译 Protobuf

```bash
# 进入 proto 目录
cd toProject/api/proto

# 生成 Go 代码
sh gen.bat
```

### 4. 启动服务

```bash
# 启动项目服务
cd toProject
go run main.go

# 启动用户服务
cd toUser
go run main.go

# 启动 API 网关
cd api
go run main.go
```

### 5. API 访问

API 网关默认运行在 `http://localhost:8080`

## API 路由

### 项目管理

| 方法 | 路径                | 说明          |
| ---- | ------------------- | ------------- |
| POST | /iteration/selfList | 我的迭代列表  |
| POST | /iteration/save     | 创建/编辑迭代 |
| POST | /iteration/read     | 迭代详情      |
| POST | /iteration/recycle  | 软删除        |
| POST | /iteration/recovery | 恢复迭代      |
| POST | /iteration/collect  | 收藏迭代      |

### 任务管理

| 方法 | 路径                   | 说明          |
| ---- | ---------------------- | ------------- |
| POST | /iteration/stages      | 任务阶段列表  |
| POST | /iteration/stages/list | 需求列表      |
| POST | /iteration/save        | 新建/编辑需求 |
| POST | /iteration/sort        | 需求排序      |
| POST | /iteration/selfList    | 我的需求      |
| POST | /iteration/read        | 需求详情      |

### 团队管理

| 方法 | 路径                 | 说明          |
| ---- | -------------------- | ------------- |
| POST | /iteration/account   | 账户列表      |
| POST | /iteration/team      | 团队列表      |
| POST | /iteration/team/save | 创建/编辑团队 |

### 数据分析

| 方法 | 路径                                    | 说明        |
| ---- | --------------------------------------- | ----------- |
| POST | /iteration/analysis/interation/delivery | 迭代交付率  |
| POST | /iteration/analysis/burndown            | 燃尽图      |
| POST | /iteration/analysis/team/load           | 团队负载    |
| POST | /iteration/analysis/ai/recommend        | AI 智能分析 |

### 权限与菜单

| 方法 | 路径                  | 说明     |
| ---- | --------------------- | -------- |
| POST | /iteration/auth       | 权限列表 |
| POST | /iteration/auth/apply | 权限申请 |
| POST | /iteration/menu       | 菜单列表 |

### 文件

| 方法 | 路径                     | 说明     |
| ---- | ------------------------ | -------- |
| POST | /iteration/file/upload   | 上传文件 |
| POST | /iteration/file/list     | 文件列表 |
| POST | /iteration/file/download | 下载文件 |

## 中间件

| 中间件        | 功能            |
| ------------- | --------------- |
| CORS          | 跨域资源共享    |
| JWT Auth      | 登录鉴权（RSA） |
| Rate Limit    | 接口限流        |
| Recovery      | 全局异常恢复    |
| Prometheus    | 监控指标        |
| PProf         | 性能分析        |
| OpenTelemetry | 链路追踪        |

## 缓存策略

- **Redis**：分布式缓存 + 分布式锁（RedSync）
- **LRU**：本地热点数据缓存
- **多级缓存**：缓存优先，降级到数据库
- **缓存击穿防护**：分布式锁保护

## 可观测性

- **Jaeger**：分布式追踪
- **Prometheus**：性能指标
- **ELK**：日志收集与分析
- **Gin Logger**：HTTP 请求日志
