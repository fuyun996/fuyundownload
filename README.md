# fuyun 下载器

多连接高速下载工具：把一个文件切成多个分段，用多条连接并行下载，并根据**实测网速自动增减连接数**逼近当前带宽上限；支持**断点续传**。纯 Go 标准库实现，零第三方依赖，单二进制跨 macOS / Windows / Linux。

```
$ fuyun https://example.com/ubuntu-24.04.iso -o ~/Downloads

ubuntu-24.04.iso  [████████████████░░░░]  62.4%  ↓ 31.2MB/s  ETA 00:41  连接 14/32
```

## 特性

- **多连接分段下载**：文件按 4–64MiB 切段，多连接并行；空闲连接通过对半"窃取"最大剩余分段，尾段也有并行度
- **自适应调度（爬山算法）**：初始 4 连接，按实测吞吐增减——提速 ≥2% 继续加、下降 >5% 回退、连续平台期收敛；收敛后周期性微调减连接修剪过冲，带宽释放时自动重新爬升
- **断点续传**：分段进度原子落盘（`<文件>.fuyun`，约 2s 一次），重跑同一命令或点"继续"即从断点续传；用 `ETag` / `Last-Modified` + `If-Range` 校验远端文件未变化，变化自动弃用旧进度
- **单连接卡死看门狗**：10s 无数据自动掐断重连；429/503 遵循 `Retry-After`，指数退避重试
- **双模式**：Web 界面（SSE 实时刷新、速度曲线、每连接迷你条）+ 命令行（终端进度条，非 TTY 自动降级为日志）
- **多任务管理**：并发任务数上限、全局连接预算、程序启动时自动导入下载目录中未完成的任务（崩溃恢复）

## 构建

```bash
go build -o fuyun .          # 当前平台
# 交叉编译示例
GOOS=windows GOARCH=amd64 go build -o fuyun.exe .
GOOS=linux   GOARCH=arm64 go build -o fuyun-linux-arm64 .
```

要求 Go 1.22+（开发了 1.24）。无任何第三方依赖。

## 使用

### Web 界面（桌面用法）

```bash
fuyun                       # 启动并自动打开浏览器（默认 127.0.0.1:8791，被占用自动换端口）
fuyun ui --port 9000 --dir /data/downloads --no-browser
```

粘贴链接 → 添加下载；可暂停 / 继续 / 停止 / 删除；右上角 ⚙ 设置下载目录、连接数、并发任务数、User-Agent、自定义请求头等（持久化到 `~/.fuyun/config.json`）。

> 仅监听 127.0.0.1。设置中的"下载目录"对新添加的任务生效。

### 命令行

```bash
fuyun <url>... [flags]

  -o, --output-dir 目录    保存目录（默认 ~/Downloads）
  -O, --output 文件名      指定文件名（仅单个链接）
  -c, --conns N            单任务最大连接数（默认 32，上限 64）
  -j, --jobs N             同时下载的任务数
  -H, --header "K: V"      附加请求头（可重复，如 Cookie）
  -A, --user-agent UA      自定义 UA
      --insecure           忽略 HTTPS 证书错误
  -q, --quiet              静默
```

**断点续传**：下载中 `Ctrl+C`（或 `SIGTERM`）会优雅暂停并保存进度；重新运行**同一命令**自动续传。Web 界面重启后也会自动导入未完成任务。

```
$ fuyun https://example.com/big.bin -o ~/Downloads
^C[fuyun] ✘ big.bin 已暂停
已暂停：重新运行同一命令将继续下载。

$ fuyun https://example.com/big.bin -o ~/Downloads   # 从 37.5% 直接继续
```

## 工作原理

```
探测(HEAD / Range: bytes=0-0)
  ├─ 支持 Range 且大小已知 → 切段(目标段数≈size/4MiB, 4~1024段)
  │    多连接 worker 循环领取分段：
  │      全部被认领且仍有空闲连接 → 把最大剩余段对半分裂（工作窃取）
  │    自适应调度器（每 2s 决策）：
  │      speed↑≥2% → +1~8 连接；speed↓>5% → 回退并收敛；平台期×2 → 收敛
  │      收敛后每 6 窗尝试 -1 连接修剪过冲；速度跌至峰值 70%×3 窗 → 重新爬升
  └─ 不支持 Range / 大小未知 → 单连接整流下载（不可续传）
```

- 写入：文件预分配（`Truncate` 到全尺寸），worker 按绝对偏移 `WriteAt`，完成 `fsync` 后删除状态文件
- 一致性：分段请求带 `If-Range`；远端文件变化 → 服务器返回 200 → 任务报错"远端文件已变更"，下次运行自动全量重下
- 断点状态：JSON（URL/大小/ETag/Last-Modified/分段表），临时文件 + rename 原子写

## 已知限制

- 不支持 FTP / BT / metalink；不支持分段级校验（依赖 TCP + 大小校验）
- 服务器不支持 Range 时无法续传（会提示并从头下载）
- 需要登录的链接用 `-H "Cookie: ..."` 传入凭证（Web 界面在设置中配置）

## 目录结构

```
main.go                 入口与参数解析（ui / CLI 双模式）
internal/core/          下载引擎：探测、分段、自适应调度、worker、断点状态、全局限流
internal/manager/       多任务管理、设置持久化、SSE 广播
internal/term/          CLI 模式与终端进度渲染
internal/ui/            Web 服务器、REST API、打开浏览器
web/                    前端（go:embed 内嵌，原生 JS 无框架）
```
