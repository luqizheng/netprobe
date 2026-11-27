# netprobe — TCP / UDP 双向通信测试工具

轻量级网络连通性测试工具，用于验证两台服务器之间指定 IP + 端口的 TCP / UDP 双向通信。包含两个独立可执行程序：

- `netprobe-server`：监听端，部署在被测服务器上。
- `netprobe-client`：发送端，部署在发起测试的服务器上。

不依赖任何外部服务或数据库。

## 1. 通信协议

每轮通信严格按以下流程执行，序号 `n` 从 1 开始递增：

```text
Client                          Server
  | -------- helo(n) ----------> |
  | <--------- ok(n) ----------- |
  | -------- ack(n) -----------> |
  | <--------- end(n) ---------- |
  |        本轮完成，等待 interval |
```

- TCP：UTF-8 文本，`\n` 分帧（正确处理粘包/拆包）。
- UDP：每条消息一个独立 Datagram（无换行），回复始终发往数据报源地址。
- UDP 丢包：超时自动重传相同序号（默认最多重试 3 次），Server 对重复 helo/ack 幂等回复 ok/end。
- Server 端使用单调时钟（`std::time::Instant`）统计每轮 `helo` → `end` 的完整握手耗时。

## 2. 安装 Rust 开发环境

```bash
# Linux / macOS
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Windows：下载并运行 rustup-init.exe（https://rustup.rs）
```

要求 Rust Stable（edition 2021+）。验证：`rustc --version && cargo --version`。

## 3. 编译

```bash
cargo build --release
```

产物位置：

- Linux：`target/release/netprobe-server`、`target/release/netprobe-client`
- Windows：`target\release\netprobe-server.exe`、`target\release\netprobe-client.exe`

可将两个文件拷贝到目标机器，无需其它运行时依赖。

### 3.1 Docker 交叉编译 Linux 版本（Windows/macOS 上使用）

在装有 Docker 的机器上，用 musl 镜像编译 Linux 静态二进制（产物在 `target-linux/release/`，不污染本机 `target/`）：

```bash
docker run --rm -v "$PWD":/work -w /work \
    -e CARGO_TARGET_DIR=/work/target-linux \
    zhcoder-docker-registry.com:8000/builder/rust:1.96-slim-musl \
    cargo build --release
```

### 3.2 CI 自动发布（GitHub Actions）

推送 `v*` 格式的 tag 时，仓库自带的 `.github/workflows/release.yml` 会自动构建并发布：

```bash
git tag v0.1.0
git push origin v0.1.0
```

发布内容（GitHub Releases 页面自动生成）：

| 产物 | 架构 | 说明 |
|---|---|---|
| `netprobe-linux-amd64.tar.gz` | x86_64 | 静态 musl 编译，含 `netprobe-server` + `netprobe-client` |
| `netprobe-linux-arm64.tar.gz` | aarch64 | 静态 musl 编译，含 `netprobe-server` + `netprobe-client` |
| `*.sha256` / `SHA256SUMS` | — | 校验文件 |

解压即可使用（无需安装任何运行时依赖）：

```bash
tar -xzf netprobe-linux-amd64.tar.gz
sha256sum -c SHA256SUMS   # 校验完整性（可选）
```

## 4. 运行

### 4.1 netprobe-server（监听端）

```bash
netprobe-server --protocol tcp --bind 0.0.0.0 --port 9000
netprobe-server --protocol udp --bind 0.0.0.0 --port 9000
```

| 参数 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `--protocol` | 否 | `tcp` | `tcp` 或 `udp` |
| `--bind` | 否 | `0.0.0.0` | 监听 IP（支持 IPv6，如 `::`） |
| `--port` | 否 | `9000` | 监听端口 |

### 4.2 netprobe-client（发送端）

```bash
netprobe-client --protocol tcp --host 192.168.2.30 --port 9000
netprobe-client --protocol udp --host 192.168.2.30 --port 9000
```

| 参数 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `--protocol` | 否 | `tcp` | `tcp` 或 `udp` |
| `--host` | 是 | 无 | 目标服务器 IP 或域名（支持 IPv6） |
| `--port` | 否 | `9000` | 目标端口 |
| `--interval-secs` | 否 | `5` | 每轮结束后的等待秒数 |
| `--timeout-secs` | 否 | `3` | 单次等待响应的超时秒数 |
| `--retries` | 否 | `3` | UDP 每步最大重试次数（首次发送之外） |

### 4.3 调整间隔 / 超时 / 重试

```bash
# 每轮间隔 10 秒，超时 2 秒，UDP 重试 5 次
netprobe-client --protocol udp --host 192.168.2.30 --port 9000 \
    --interval-secs 10 --timeout-secs 2 --retries 5
```

轮次间隔语义：**一轮通信成功或失败判定结束后，等待 `--interval-secs` 再开始下一轮**；实际发送间隔 = 一轮耗时 + 等待时间。

## 5. Linux 防火墙放行

必须放行对应端口的 TCP 或 UDP（按实际使用的协议与端口调整）：

```bash
# firewalld
sudo firewall-cmd --add-port=9000/tcp --permanent && sudo firewall-cmd --reload
sudo firewall-cmd --add-port=9000/udp --permanent && sudo firewall-cmd --reload

# ufw
sudo ufw allow 9000/tcp
sudo ufw allow 9000/udp

# iptables
sudo iptables -A INPUT -p tcp --dport 9000 -j ACCEPT
sudo iptables -A INPUT -p udp --dport 9000 -j ACCEPT
```

云服务器还需在安全组中放行对应端口。本工具不会自动修改任何防火墙配置。

## 6. 实际双服务器测试步骤

假设：Server 机器 `192.168.2.30`，Client 机器 `192.168.2.31`。

### 6.1 TCP 测试

```bash
# 192.168.2.30
./netprobe-server --protocol tcp --bind 0.0.0.0 --port 9000

# 192.168.2.31
./netprobe-client --protocol tcp --host 192.168.2.30 --port 9000
```

预期：Client 打印 `Connected to 192.168.2.30:9000`，每轮 `helo → ok → ack → end` 并显示 `Round N SUCCESS`；Server 显示 `Client connected: 192.168.2.31:<port>`、每条收发消息及 `Elapsed` 耗时。Ctrl+C 正常退出。

### 6.2 UDP 测试

```bash
# 192.168.2.30
./netprobe-server --protocol udp --bind 0.0.0.0 --port 9000

# 192.168.2.31
./netprobe-client --protocol udp --host 192.168.2.30 --port 9000
```

预期：Server 首次收到数据报时打印 `New client detected: 192.168.2.31:<port>`，随后每轮正常完成。

### 6.3 自定义端口测试

```bash
# 双端均使用 9100
./netprobe-server --protocol tcp --bind 0.0.0.0 --port 9100
./netprobe-client --protocol tcp --host 192.168.2.30 --port 9100
```

### 6.4 多轮连续通信测试

Client 持续运行不自动退出，观察至少 3 轮以上 `Round N SUCCESS` 且序号连续递增；TCP 全程只建立一次连接（Server 只打印一次 `Client connected`）。

### 6.5 UDP 超时与重传测试

方法一：临时在 Server 侧用 iptables 丢弃部分入站 UDP（测试后删除规则）：

```bash
sudo iptables -I INPUT -p udp --dport 9000 -m statistic --mode nth --every 2 --packet 1 -j DROP
```

方法二：直接断开/暂停 Server。预期 Client 日志出现 `timeout waiting for ok(n), retry 1/3 ...`，恢复后同一序号继续完成本轮；若超过重试次数则打印 `Round N FAILED`，等待间隔后继续第 N+1 轮，进程不退出。

## 7. Windows 与 Linux 的差异

| 项目 | Linux | Windows |
|---|---|---|
| 可执行文件 | `netprobe-server` / `netprobe-client` | `netprobe-server.exe` / `netprobe-client.exe` |
| 编译命令 | `cargo build --release` | 相同（建议在 PowerShell / Windows Terminal 中执行） |
| 跨平台编译 | `rustup target add x86_64-pc-windows-msvc`（完整交叉编译需 MSVC 工具链，一般直接在 Windows 上编译） | `rustup target add x86_64-unknown-linux-gnu`（链接 Linux 二进制通常需在 Linux 或容器中完成） |
| 防火墙 | firewalld / ufw / iptables（见第 5 节） | Windows Defender 防火墙首次运行会弹出放行提示，或在"高级安全 Windows 防火墙"中为入站 TCP/UDP 9000 添加规则 |
| 路径分隔 | `./netprobe-server ...` | `.\netprobe-server.exe ...` |

功能行为（协议、日志、计时、重传）在两个平台完全一致。

## 8. 项目结构

```text
netprobe/
├── Cargo.toml
├── README.md
├── src/
│   ├── lib.rs                 # 模块入口 + tracing 初始化
│   ├── protocol.rs            # 消息解析 / 状态机 RoundTracker（TCP、UDP 复用）
│   ├── server.rs              # TCP / UDP 监听端实现
│   ├── client.rs              # TCP / UDP 发送端实现
│   └── bin/
│       ├── netprobe-server.rs # 监听端 CLI 入口
│       └── netprobe-client.rs # 发送端 CLI 入口
└── tests/
    ├── tcp_integration.rs     # TCP 集成测试
    └── udp_integration.rs     # UDP 集成测试（含丢包重传、幂等、多客户端）
```

## 9. 日志说明

- `[RECV]` / `[SEND]`：收发的每条协议消息（含对端 IP:端口与序号）。
- `[INFO] Round: N` + `Status: SUCCESS` + `Elapsed: x.xxx ms`：Server 端每轮完整握手耗时（`Instant` 单调时钟，毫秒、三位小数）。
- `[WARN]`：非法消息、序号不匹配、重复消息、超时重传、连接中断等异常；均为 WARN/ERROR，不会导致进程退出。

## 10. 开发与测试

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```