package com.imagate

import android.app.Activity
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.Typeface.BOLD
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets

/**
 * ImaGate 独立模块 UI
 * - 网关状态（实时轮询 /__status）
 * - 接入信息（Base URL / API Key / 一键复制）
 * - 局域网映射配置（开关 + 密钥，重启 IMA 生效）
 * - 模型列表（/v1/models 实时拉取）
 * - 快捷自测（真实推理链路）
 */
class MainActivity : Activity() {

    private val IMA_BASE = "http://127.0.0.1:8731"
    private lateinit var statusText: TextView
    private lateinit var modelsText: TextView
    private lateinit var lanKeyInput: EditText
    private lateinit var lanStatusText: TextView
    private lateinit var testText: TextView
    private lateinit var testBtn: Button
    private val handler = Handler(Looper.getMainLooper())
    private var polling = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        buildUi()
        refreshStatus()
    }

    override fun onResume() {
        super.onResume()
        startPolling()
    }

    override fun onPause() {
        super.onPause()
        polling = false
        handler.removeCallbacksAndMessages(null)
    }

    private fun dp(v: Int) = (v * resources.displayMetrics.density).toInt()

    private fun buildUi() {
        val root = ScrollView(this).apply { setBackgroundColor(Color.parseColor("#10141B")) }
        val box = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(20), dp(24), dp(20), dp(24))
        }
        root.addView(box)

        fun title(text: String, size: Float = 22f): TextView = TextView(this).apply {
            this.text = text; textSize = size; setTextColor(Color.WHITE); setTypeface(typeface, BOLD)
        }
        fun card(): LinearLayout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(Color.parseColor("#1A2130"))
            setPadding(dp(16), dp(14), dp(16), dp(14))
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT).apply { topMargin = dp(12) }
        }
        fun label(text: String): TextView = TextView(this).apply {
            this.text = text; textSize = 12f; setTextColor(Color.parseColor("#7FA7D9")); setTypeface(typeface, BOLD)
        }
        fun value(text: String, mono: Boolean = true): TextView = TextView(this).apply {
            this.text = text; textSize = 14f; setTextColor(Color.parseColor("#E8EEF7"))
            typeface = if (mono) Typeface.MONOSPACE else Typeface.DEFAULT
            setTextIsSelectable(true)
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT).apply { topMargin = dp(4) }
        }
        fun copyBtn(text: String, getText: () -> String): Button = Button(this).apply {
            this.text = text; textSize = 12f
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(36)).apply { topMargin = dp(6) }
            setOnClickListener {
                val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                cm.setPrimaryClip(ClipData.newPlainText("imagate", getText()))
                Toast.makeText(this@MainActivity, "已复制", Toast.LENGTH_SHORT).show()
            }
        }

        // 标题
        box.addView(title("ImaGate", 24f))
        box.addView(TextView(this).apply {
            text = "腾讯 IMA 内置模型 → 本机/局域网 OpenAI 兼容 API · v1.0 独立模块"
            textSize = 12f; setTextColor(Color.parseColor("#8B96A8"))
        })

        // 状态卡
        val c1 = card()
        statusText = value("…")
        c1.addView(label("网关状态（实时轮询 /__status）")); c1.addView(statusText)
        box.addView(c1)

        // 接入信息卡
        val c2 = card()
        c2.addView(label("Base URL（本机/agent 壳）"))
        c2.addView(value("$IMA_BASE/v1"))
        c2.addView(label("PC 接入（adb 转发）").apply { layoutParams = LinearLayout.LayoutParams(-2, -2).apply { topMargin = dp(10) } })
        c2.addView(TextView(this).apply {
            text = "adb forward tcp:18731 tcp:8731\nPC 访问 http://127.0.0.1:18731/v1"
            textSize = 12f; setTextColor(Color.parseColor("#9AA6B8")); typeface = Typeface.MONOSPACE
            setLineSpacing(0f, 1.3f)
            layoutParams = LinearLayout.LayoutParams(-2, -2).apply { topMargin = dp(4) }
        })
        c2.addView(copyBtn("📋 复制 adb 转发命令", { "adb forward tcp:18731 tcp:8731" }))
        box.addView(c2)

        // LAN 配置卡
        val c3 = card()
        c3.addView(label("局域网映射（电脑直连手机 IP:8731）"))
        lanStatusText = TextView(this).apply {
            textSize = 12f; setTextColor(Color.parseColor("#C9D5E5")); setLineSpacing(0f, 1.25f)
            layoutParams = LinearLayout.LayoutParams(-2, -2).apply { topMargin = dp(4) }
        }
        c3.addView(lanStatusText)
        lanKeyInput = EditText(this).apply {
            hint = "API Key（LAN 模式必填，客户端用 Bearer 或 x-api-key 携带）"
            textSize = 13f
            setTextColor(Color.parseColor("#E8EEF7")); setHintTextColor(Color.parseColor("#55617A"))
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT).apply { topMargin = dp(8) }
        }
        c3.addView(lanKeyInput)
        val lanSave = Button(this).apply {
            text = "💾 保存 LAN 配置（重启 IMA App 后生效）"
            textSize = 13f
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(42)).apply { topMargin = dp(8) }
            setOnClickListener { saveLan() }
        }
        c3.addView(lanSave)
        c3.addView(TextView(this).apply {
            text = "⚠ LAN 模式将 API 暴露到局域网（0.0.0.0），务必设置强密钥。本机 127.0.0.1 始终免鉴权。"
            textSize = 11f; setTextColor(Color.parseColor("#8A7A4A")); setLineSpacing(0f, 1.2f)
            layoutParams = LinearLayout.LayoutParams(-2, -2).apply { topMargin = dp(6) }
        })
        box.addView(c3)

        // 模型列表卡
        val c4 = card()
        c4.addView(label("模型列表（/v1/models 实时拉取）"))
        modelsText = TextView(this).apply {
            text = "下拉刷新或点击按钮拉取"
            textSize = 12f; setTextColor(Color.parseColor("#C9D5E5")); typeface = Typeface.MONOSPACE
            setLineSpacing(0f, 1.3f)
            layoutParams = LinearLayout.LayoutParams(-2, -2).apply { topMargin = dp(4) }
        }
        c4.addView(modelsText)
        val modelsBtn = Button(this).apply {
            text = "🔄 拉取模型列表"
            textSize = 13f
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(40)).apply { topMargin = dp(8) }
            setOnClickListener { fetchModels() }
        }
        c4.addView(modelsBtn)
        box.addView(c4)

        // 自测卡
        val c5 = card()
        c5.addView(label("快捷自测（/v1/chat/completions 真实推理）"))
        testBtn = Button(this).apply {
            text = "▶ 发送测试问题"
            textSize = 13f
            layoutParams = LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, dp(40)).apply { topMargin = dp(8) }
            setOnClickListener { selfTest() }
        }
        c5.addView(testBtn)
        testText = TextView(this).apply {
            text = "点击发起一次完整推理（凭证捕获→IMA 服务端→流式聚合）"
            textSize = 12f; setTextColor(Color.parseColor("#C9D5E5")); setLineSpacing(0f, 1.25f)
            layoutParams = LinearLayout.LayoutParams(-2, -2).apply { topMargin = dp(6) }
        }
        c5.addView(testText)
        box.addView(c5)

        setContentView(root)
    }

    // ---- 状态轮询 ----
    private fun startPolling() {
        if (polling) return
        polling = true
        handler.postDelayed(object : Runnable {
            override fun run() {
                refreshStatus()
                if (polling) handler.postDelayed(this, 5000)
            }
        }, 0)
    }

    private fun refreshStatus() {
        Thread {
            var shown = ""
            try {
                val (code, body) = httpGet("$IMA_BASE/__status", 4000)
                if (code == 200) {
                    fun pick(k: String): String {
                        val m = Regex("\"$k\"\\s*:\\s*\"?([^,\"}]+)\"?").find(body)
                        return m?.groupValues?.get(1) ?: "-"
                    }
                    val creds = if (body.contains("\"hasCreds\":true")) "● 已捕获" else "○ 待捕获（打开 IMA 触发联网）"
                    val ts = pick("credTs").toLongOrNull()
                    val tsStr = if (ts != null && ts > 0) android.text.format.DateFormat.format("MM-dd HH:mm", ts).toString() else "-"
                    shown = "● 运行中 · 端口 8731 · ${pick("gw")}\n" +
                        "凭证: $creds（更新于 $tsStr）\n" +
                        "会话: ${pick("sessions")} · 模型: ${pick("model_id")}\n" +
                        "session: ${pick("sessionId").take(20)}…" +
                        (if (pick("lastErr") != "-") "\n⚠ lastErr: ${pick("lastErr").take(100)}" else "")
                } else shown = "○ 网关响应异常 HTTP $code"
            } catch (t: Throwable) {
                shown = "○ 网关不可达（IMA 未运行或未注入）\n请先打开腾讯 IMA App"
            }
            val s = shown
            runOnUiThread {
                statusText.text = s
                refreshLanLabel()
            }
        }.start()
    }

    private fun refreshLanLabel() {
        // LAN 状态从 /__status 不可见（配置在文件里），显示手机 IP 提示
        try {
            val ni = java.net.NetworkInterface.getNetworkInterfaces()
            var ip = ""
            for (n in ni) {
                for (a in n.inetAddresses) {
                    if (!a.isLoopbackAddress && a is java.net.Inet4Address) { ip = a.hostAddress ?: ""; break }
                }
                if (ip.isNotEmpty()) break
            }
            lanStatusText.text = if (ip.isNotEmpty())
                "手机 IP: $ip（LAN 开启后 PC 访问 http://$ip:8731/v1，需 API Key）\n配置存于 IMA 进程，重启 IMA App 生效"
            else "未获取到 WiFi IP"
        } catch (_: Throwable) {}
    }

    // ---- LAN 保存（写入网关配置文件；网关在 IMA 进程内，本 UI 进程无法直接写其 files——
    //      改为通过网关的配置端点？IMA 进程独享 files。方案：写自身的配置副本由网关 hook 读取？
    //      实际方案：直接写到 IMA files 需要 IMA 进程权限——这里通过网关 __config 端点下发）----
    private fun saveLan() {
        val key = lanKeyInput.text.toString().trim()
        Thread {
            var msg: String
            try {
                // 通过网关的 /__config 端点写配置（网关进程内执行文件写入，权限正确）
                val (code, resp) = httpPost("$IMA_BASE/__config",
                    "{\"lan_enabled\":true,\"api_key\":\"$key\"}", 8000)
                msg = if (code == 200) "LAN 配置已保存 ✓ 重启 IMA App 后生效\n$resp" else "保存失败 HTTP $code: $resp"
            } catch (t: Throwable) { msg = "保存失败: ${t.message?.take(120)}（IMA 未运行？）" }
            val m = msg
            runOnUiThread {
                Toast.makeText(this, m, Toast.LENGTH_LONG).show()
                refreshStatus()
            }
        }.start()
    }

    // ---- 模型列表 ----
    private fun fetchModels() {
        Thread {
            var shown: String
            try {
                val (code, body) = httpGet("$IMA_BASE/v1/models", 30000)
                if (code == 200) {
                    val ids = Regex("\"id\"\\s*:\\s*\"([^\"]+)\"").findAll(body).map { it.groupValues[1] }.toList()
                    shown = ids.joinToString("\n") { "· $it" }.ifEmpty { "(空)" }
                } else shown = "HTTP $code"
            } catch (t: Throwable) { shown = "拉取失败: ${t.message?.take(100)}" }
            val s = shown
            runOnUiThread { modelsText.text = s }
        }.start()
    }

    // ---- 自测 ----
    private fun selfTest() {
        if (testBtn.isEnabled == false) return
        testBtn.isEnabled = false
        testBtn.text = "⏳ 推理中…"
        testText.text = "已发出，等待 IMA 响应…"
        Thread {
            val t0 = System.currentTimeMillis()
            var result: String
            try {
                val body = "{\"model\":\"auto\",\"stream\":false,\"messages\":[{\"role\":\"user\",\"content\":\"1+1=? 只回答数字\"}]}"
                val (code, resp) = httpPost("$IMA_BASE/v1/chat/completions", body, 90000)
                val ms = System.currentTimeMillis() - t0
                if (code == 200) {
                    val m = Regex("\"content\"\\s*:\\s*\"([^\"]*)\"").find(resp)
                    result = "✅ HTTP 200 · ${ms}ms\n回答: ${m?.groupValues?.get(1) ?: "(空)"}"
                } else result = "❌ HTTP $code · ${ms}ms\n${resp.take(200)}"
            } catch (t: Throwable) { result = "❌ ${t.message?.take(150)}" }
            val r = result
            runOnUiThread {
                testText.text = r
                testBtn.isEnabled = true
                testBtn.text = "▶ 发送测试问题"
            }
        }.start()
    }

    // ---- HTTP 小工具 ----
    private fun httpGet(url: String, timeoutMs: Int): Pair<Int, String> {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = timeoutMs; c.readTimeout = timeoutMs
        c.requestMethod = "GET"
        val code = c.responseCode
        val text = (if (code >= 400) c.errorStream else c.inputStream)?.bufferedReader(StandardCharsets.UTF_8)?.use { it.readText() } ?: ""
        return Pair(code, text)
    }

    private fun httpPost(url: String, body: String, timeoutMs: Int): Pair<Int, String> {
        val c = URL(url).openConnection() as HttpURLConnection
        c.connectTimeout = timeoutMs; c.readTimeout = timeoutMs
        c.requestMethod = "POST"; c.doOutput = true
        c.setRequestProperty("Content-Type", "application/json")
        c.outputStream.use { it.write(body.toByteArray(StandardCharsets.UTF_8)) }
        val code = c.responseCode
        val text = (if (code >= 400) c.errorStream else c.inputStream)?.bufferedReader(StandardCharsets.UTF_8)?.use { it.readText() } ?: ""
        return Pair(code, text)
    }
}
