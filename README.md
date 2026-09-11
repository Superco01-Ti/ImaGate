# ImaGate

把腾讯 IMA（腾讯元宝系 App）内置的模型，通过 Xposed Hook 抓取凭证后，在设备本地重放其私有协议，对外暴露成标准 OpenAI 兼容 API 的**逆向反代网关**。

用它，你可以拿任意 OpenAI 客户端（Codex、Claude Code、Cline 等）直接调用 IMA 内置的 DeepSeek-V4、GLM-5.3、Kimi-K2.7 等模型。

> ⚠️ 逆向产物，仅供学习研究，请勿商用、勿滥用。

## 它到底怎么工作的

整条链路是「Hook 抓凭证 → 本地反代 → 协议重放 → SSE 翻译」：

1. **Hook 抓凭证**（`ImaHook.kt`）

   Xposed 模块注入 `com.tencent.ima` 进程，hook 出站请求（`kw.b`、`jw.k`、`tg0.i0` 等混淆类，多候选兜底），把 `x-ima-cookie`、`x-ima-bkn`、`referer`、`origin` 真实请求头抓下来当"门票"。凭证落盘持久化，杀进程后重启自动恢复。

2. **本地反代**（`ImGate.kt` / `ImatGateway.kt`）

   在 `127.0.0.1:8731`（可切 LAN + API Key）起本地 HTTP 服务，用抓到的凭证 `HttpURLConnection` 重放 IMA 私有接口：

   - `init_session` 建会话拿 `session_id`
   - `/cgi-bin/assistant/qa` 发问题，SSE 流式收 `MESSAGE` / `THINKING` / `QA_START` / `HEARTBEAT`
   - `/cgi-bin/model_manage/get_models` 扫各 scene 分区拉真实模型表

   完全绕开 IMA 内部网络对象，不碰加密。

3. **伪装 OpenAI 接口**

   对外暴露 `/v1/chat/completions`、`/v1/models`、`/v1/responses` 等标准接口，把私有 SSE 帧流式翻译成 `chat.completion.chunk`。思考帧透传为 `reasoning_content`，`【思考】...【/思考】` 标记自动重铸。

## 能力

- ✅ **多轮会话**：服务端记忆模式（`history_type=ALL_HISTORY`）+ 无状态模式，历史健康度日志排查"健忘"
- ✅ **思考透传**：THINKING 帧 / 【思考】标记 / `-think` 后缀变体，三通道齐全
- ✅ **工具调用**：Operit 原生 XML 格式注入与重铸，工具结果回填，自动移除坏格式[1]
- ✅ **模型别名**：把展示名挂到真实付费/免费端点（`official_paid_ep-*`、免费区双档表），`-web` / `-think` 任意组合派生变体
- ✅ **识图**：OpenAI 多模态 `image_url` → base64 解码/下载 → 上传 IMA（media_type=9）→ `parse_media` 同构复用
- ✅ **文件通道**：超长上下文文本文件化（COS 预签名直传 + `parse_media`），内容哈希缓存复用
- ✅ **DNS 看门狗**：IMA 进程 DNS 挂起自动 force-stop + 重启自愈（10 分钟冷却）
- ✅ **会话管理**：每请求强制新会话 + 全局序号 + 每日清理 + 会话诊断端点
- ✅ **LAN 模式 + API Key**：可绑 `0.0.0.0` 开局域网访问

## ⚠️ 已知限制（务必先读）

- **IMA 原生 web 联网（`-web` / enhance）与图片上传的可用性较低**。
- 开启这些功能**有可能导致上下文丢失**（服务端老会话"图片缓存顶包"、联网场景历史不完整等，代码里已做了部分强声明/强制新会话补救，但仍不保证）。
- **请自行验证后再决定是否启用，谨慎使用。**

## 环境要求

- Android 10 及以上，已 root，装了 LSPosed 或同类框架
- 目标 App：`com.tencent.ima`
- Kotlin 1.9+

## 怎么用

### 方式一：装 APK

1. 去 [Releases](https://github.com/Superco01-Ti/ImaGate/releases) 下载最新版 APK
2. 安装后在 LSPosed 里激活模块
3. 勾选作用目标 App `com.tencent.ima`，重启该 App

### 方式二：自己构建

```bash
git clone https://github.com/Superco01-Ti/ImaGate.git
cd ImaGate
./gradlew assembleRelease
```

## 目录结构

```
app/src/main/java/com/imagate/
├── GateEntry.kt      # Xposed 模块入口
├── ImGate.kt         # 网关核心：会话/模型/上传/重放（v1.7.x）
├── ImaHook.kt        # hook 抓凭证：多候选混淆类 + 凭证持久化
├── ImatGateway.kt    # 轻量网关 v0.x：协议重放 + SSH 流式服务
└── MainActivity.kt   # 控制台界面：状态/自测/LAN 配置
```

## License

[GPL-3.0](LICENSE)

> 仅供学习研究使用，请勿用于任何非法用途。
