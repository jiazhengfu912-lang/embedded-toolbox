# Embedded Toolbox

[English](README.md) | 中文

面向 Windows 的嵌入式桌面工作台，用于 STM32、ESP32、Arduino 及其他 USB-UART 设备的调试、数据采集、协议分析和手动 PID 调参。

技术栈包括 Tauri 2、Rust、React、TypeScript、uPlot 和 SQLite。

![Embedded Toolbox 界面](artifacts/ui-preview.png)

## 功能

- 串口发现；Windows V1 产品界面限制为一个活动串口源，Rust 核心则围绕支持多源的 `SourceRegistry` 设计。
- Terminal、Plotter、Packets 和 PID Tuner 共用采集管线，同时为各消费端提供独立的有界队列。
- 通过 FIFO `TxScheduler` 提供文本和十六进制终端收发，支持截止时间、取消、最小帧间隔、部分写入记录和 TX 状态事件。
- 支持结束分隔符、帧头/帧尾、固定长度和长度字段组帧，并提供最大帧长限制和重同步规则。
- 支持 CSV、JSON 和二进制解码；CRC/XOR/SUM 校验范围可配置；有状态变换具有明确的重置原因。
- 使用类型化数组环形缓冲区和基于像素桶的最小/最大值降采样绘制时序数据。
- 使用 SQLite WAL 保存会话，支持周期性被动 checkpoint、结束时 truncate checkpoint、多 epoch 会话、回放和 CSV 导出。
- 使用带版本的二进制流 envelope，并通过 `runtimeInstanceId`、`EventCursor` 和快照同步控制面状态。
- 提供确定性的合成传输，可注入断开、损坏、丢帧、重复、分片、突发、写入失败和部分写入故障。

## 架构

```text
SourceRuntime
  |- Transport RX
  |- SessionClock
  |- TxScheduler
  |- RecorderQueue -> SQLite
  |- ParserQueue -> Framer -> Decoder -> Transform
  `- DisplayQueue -> Tauri IPC -> React
```

每个下游消费端拥有独立的有界队列。缓慢的 UI、解析器或记录器不会把背压静默传播到无关消费端。

## 技术栈

- 桌面外壳：Tauri 2
- 核心：Rust stable
- 前端：React、TypeScript、Vite、pnpm
- 绘图：uPlot
- 存储：SQLite WAL
- 目标平台：Windows 10/11 x64

## 快速开始

前置要求：Node.js 24、pnpm 10、Rust stable、Visual Studio 2022 C++ Build Tools，以及 Microsoft Edge WebView2 Runtime。

```powershell
git clone https://github.com/jiazhengfu912-lang/embedded-toolbox.git
cd embedded-toolbox
pnpm install
pnpm tauri dev
```

仅预览带有内置合成遥测数据的 React UI：

```powershell
pnpm dev
```

## 硬件快速测试

1. 使用 3.3 V USB-UART 转换器连接 STM32、ESP32 或 Arduino。
2. 在 **Transport** 中选择转换器的 COM 端口和波特率。
3. 选择 **Connect**，并在 **Terminal** 中确认收到数据。
4. 打开 **Plotter** 查看 CSV/JSON 样本，打开 **Packets** 查看已组帧的二进制数据，或使用 **PID Tuner** 将 PID 命令加入发送队列。
5. 选择 **Record session**，将会话保存为 SQLite `.etdb` 文件。

仓库内附带的 STM32F103C8T6 遥测固件位于 [`Test/STM32F103_Telemetry`](Test/STM32F103_Telemetry)。

## 验证

```powershell
pnpm test
pnpm typecheck
pnpm build

Push-Location .\src-tauri
cargo test --lib
Pop-Location

pnpm tauri build
```

2026-08-16 已使用 STM32F103C8T6 和 CH340 USB-UART 转换器在 115200 波特率下完成以下硬件验证：

- 真实串口采集、Terminal、Plotter、Packets 和基本 PID 命令流程。
- 会话记录和最终 SQLite WAL truncate checkpoint。
- 记录期间拔出并重新插入 USB-UART：Epoch 1 以 `TransportFault` 结束，应用在重新连接的 `COM10` 设备上自动创建 Epoch 2 并继续记录。

以下验收项目仍待完成：921600 波特率 30 分钟压力测试、完整 TX 调度故障矩阵、全部组帧/校验变体、回放定位/版本拒绝、EventCursor 重连边界情况、UI 内存限制测试，以及 COM 端口号变化后的重连测试。

## 项目结构

```text
src/                         React 前端
src-tauri/src/               Rust 核心和 Tauri IPC
Test/STM32F103_Telemetry/    STM32F103C8T6 遥测固件
examples/                    示例 .etp 项目
docs/ARCHITECTURE.md         核心架构和数据约定
artifacts/                   UI 预览图片
```

## 构建安装程序

```powershell
pnpm tauri build
```

生成的安装程序位于 `src-tauri/target/release/bundle/`。

## V1 范围

V1 支持 Windows、产品界面中的一个活动串口源、四个固定工作区和手动 PID 调参。BLE、TCP/UDP、CAN、Modbus、多设备 UI、脚本/插件、自动 PID 调参、固件烧录和云功能明确不在 V1 范围内。

## 许可证

项目尚未选择开源许可证。添加许可证之前，保留所有权利。
