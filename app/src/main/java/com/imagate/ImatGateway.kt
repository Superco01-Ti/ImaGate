package com.imagate

import android.util.Log
import org.json.JSONArray
import org.json.JSONObject
import java.io.InputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

/**
 * ImatGateway v0.3 (Route A + multi-turn + models) - inside com.tencent.ima process.
 * Local OpenAI-compatible API on 127.0.0.1:8731 backed by IMA models.
 *
 * Execution model: ImaHook captures real outbound request headers (x-ima-cookie,
 * x-ima-bkn, referer...) at tg0.i0.b(Request) and feeds them via updateCreds().
 * We then REPLAY plain JSON/SSE to ima.qq.com with HttpURLConnection - no calls
 * into IMA internal network objects, no crypto.
 *
 * Protocol (verified by capture 2026-09-06):
 *  init_session POST /cgi-bin/session_logic/init_session
 *    {"env_info":{"interact_type":2,"robot_type":10000},"name":"<firstQ>","msgs_limit":10}
 *  ask  SSE POST /cgi-bin/assistant/qa
 *    {"session_id":"...","robot_type":10000,"question":"...","question_type":1,
 *     "command_info":{"question_info":{}},"client_id":"<uuid>",
 *     "model_info":{"model_type":N,"enable_enhancement":true,"model_id":"<id>"}}
 *  SSE: QA_START/SESSION_START/SEARCH_MEDIAS/HEARTBEAT/MESSAGE(data {"Text":".."})
 */
object ImatGateway {

    const val PORT = 8731
    const val API = "https://ima.qq.com"
    @Volatile var logToXposed: (String) -> Unit = {}
    private val running = AtomicBoolean(false)
    private val lock = Any()

    @Volatile var sessionId: String = ""
    @Volatile var lastErr = ""
    @Volatile var clientId = UUID.randomUUID().toString()
    @Volatile var modelType = 3000
    @Volatile var modelId = "official_3000"
    @Volatile var modelName = "ima-default"
    @Volatile var enhance = false
    @Volatile var credTs = 0L
    private var creds: Map<String, String> = emptyMap()
    private val sessions = ConcurrentHashMap<String, String>()
    private val pool = Executors.newCachedThreadPool()
    @Volatile private var modelsCache: org.json.JSONArray? = null
    @Volatile private var modelsTs = 0L
    // v0.3.1: 可读别名 -> (真实 model_id, model_type)；/v1/models 对外展示别名，chat 时反查
    private val modelAlias = java.util.concurrent.ConcurrentHashMap<String, Pair<String, Int>>()
    // v0.3.5: DNS watchdog（IMA 进程 DNS 挂起时自动重启自身进程）
    @Volatile private var dnsFailStreak = 0
    @Volatile private var lastAutoRestart = 0L

    fun start(classLoader: ClassLoader) {
        if (!running.compareAndSet(false, true)) return
        Thread({ serve() }, "imat-gw").apply { isDaemon = true }.start()
    }

    fun updateCreds(m: Map<String, String>) {
        if (m.isEmpty()) return
        synchronized(lock) {
            val merged = HashMap<String, String>()
            merged.putAll(creds)
            merged.putAll(m)
            creds = merged
            credTs = System.currentTimeMillis()
        }
    }

    private fun credsNow(): Map<String, String> = synchronized(lock) { creds }

    private fun log(s: String) { try { logToXposed("[ImatGW] " + s) } catch (_: Throwable) {} }

    private fun postJson(url: String, json: String): String {
        val c = URL(url).openConnection() as HttpURLConnection
        c.requestMethod = "POST"
        c.connectTimeout = 20000
        c.readTimeout = 60000
        c.doOutput = true
        for ((k, v) in credsNow()) {
            if (!k.equals("Content-Type", true) && !k.equals("content-length", true) && !k.equals("accept", true)) c.setRequestProperty(k, v)
        }
        c.setRequestProperty("Content-Type", "application/json; charset=utf-8")
        c.setRequestProperty("Accept", "application/json")
        val out: OutputStream = c.outputStream
        out.write(json.toByteArray(Charsets.UTF_8)); out.flush(); out.close()
        val code = c.responseCode
        val txt = (if (code in 200..299) c.inputStream else c.errorStream)?.bufferedReader(Charsets.UTF_8)?.use { it.readText() } ?: ""
        if (code !in 200..299) log("postJson HTTP " + code + " " + url + " body=" + txt.take(300))
        c.disconnect()
        return txt
    }

    /** SSE ask; returns accumulated text. v0.4.5: onThink 回调输出思考增量（THINKING 帧 Message 分片） */
    private fun askStream(url: String, json: String, onDelta: (String) -> Unit, onThink: (String) -> Unit = {}): String {
        val c = URL(url).openConnection() as HttpURLConnection
        c.requestMethod = "POST"
        c.connectTimeout = 20000
        c.readTimeout = 600000
        c.doOutput = true
        for ((k, v) in credsNow()) {
            if (!k.equals("Content-Type", true) && !k.equals("content-length", true) && !k.equals("accept", true)) c.setRequestProperty(k, v)
        }
        c.setRequestProperty("Content-Type", "application/json; charset=utf-8")
        c.setRequestProperty("Accept", "text/event-stream")
        log("ask start " + url)
        try {
        val out: OutputStream = c.outputStream
        out.write(json.toByteArray(Charsets.UTF_8)); out.flush(); out.close()
        val code = c.responseCode
        val full = StringBuilder()
        val think = StringBuilder()
        var thinkDone = false
        var thinkEndNotified = false
        var events = 0
        var firstPayload = ""
        if (code !in 200..299) {
            val err = c.errorStream?.bufferedReader(Charsets.UTF_8)?.use { it.readText() } ?: ""
            log("ask HTTP " + code + " err=" + err.take(500))
            c.disconnect(); return ""
        }
        log("ask HTTP 200, reading stream")
        val br = BufferedReader(InputStreamReader(c.inputStream, Charsets.UTF_8))
        var line = br.readLine()
        var curEvent = ""
        while (line != null) {
            val t = line.trim()
            if (t.startsWith("event:")) { curEvent = t.removePrefix("event:").trim(); line = br.readLine(); continue }
            if (t.startsWith("data:")) {
                val payload = t.removePrefix("data:").trim()
                val type = curEvent.ifEmpty { "" }
                curEvent = ""
                if (payload.isNotEmpty() && payload != "[DONE]") {
                    events++
                    if (firstPayload.isEmpty()) firstPayload = type + "|" + payload.take(160)
                    try {
                        val o = JSONObject(payload)
                        if (type == "MESSAGE") {
                            val txt = o.optString("Text", "")
                            if (txt.isNotEmpty()) { full.append(txt); onDelta(txt) }
                            if (full.length < 2000) log("MSG " + payload.take(300))
                        } else if (type == "THINKING") {
                            // v0.4.5: 思考帧 Status=1+Message=思考增量分片；Status=2=思考结束
                            val msg = o.optString("Message", "")
                            val status = o.optInt("Status", 0)
                            if (msg.isNotEmpty()) { think.append(msg); onThink(msg) }
                            if (status == 2 && thinkEndNotified) { /* already */ }
                            if (status == 2) thinkDone = true
                        } else if (type == "QA_START") {
                            log("qa_start " + payload.take(200))
                        }
                    } catch (_: Throwable) {}
                }
            }
            line = br.readLine()
        }
        c.disconnect()
        log("ask done events=" + events + " len=" + full.length)
        return full.toString()
    } catch (t: Throwable) {
        log("askStream EX " + t + "\n" + Log.getStackTraceString(t).take(1000))
        try { c.disconnect() } catch (_: Throwable) {}
        return ""
    }
    }

    // ---------- IMA protocol replay ----------
    private fun initSession(name: String): String {
        var attempt = 0
        while (attempt < 3) {
            attempt++
            try {
                if (credsNow().isEmpty()) { lastErr = "no creds captured yet"; Thread.sleep(2000); continue }
                val body = JSONObject()
                body.put("env_info", JSONObject().put("interact_type", 2).put("robot_type", 10000))
                body.put("name", name)
                body.put("msgs_limit", 10)
                val txt = postJson(API + "/cgi-bin/session_logic/init_session", body.toString())
                log("init_session resp: " + txt.take(400))
                val sid = extractSession(txt)
                if (sid.isNotEmpty()) { dnsFailStreak = 0; sessionId = sid; return sid }
            } catch (t: Throwable) {
                log("init_session err: " + t + "\n" + Log.getStackTraceString(t).take(1200))
                if (t.toString().contains("UnknownHostException")) {
                    dnsFailStreak++
                    lastErr = "IMA 进程 DNS 失效（UnknownHostException）→ 自动重启 IMA 中（第 $dnsFailStreak 次）"
                } else lastErr = t.toString()
            }
            Thread.sleep(3000)
        }
        // v0.3.5 watchdog: 连续 DNS 失败 = IMA 进程网络栈坏死，自动重启进程自愈（10 分钟冷却防循环）
        if (dnsFailStreak >= 3) autoRestartIma()
        return ""
    }

    private fun autoRestartIma() {
        val now = System.currentTimeMillis()
        if (now - lastAutoRestart < 600_000L) { log("auto-restart skipped (10min cooldown)"); return }
        lastAutoRestart = now
        log("WATCHDOG: DNS 连续失败 $dnsFailStreak 次 -> 自动重启 IMA 进程自愈...")
        try {
            Runtime.getRuntime().exec(arrayOf("am", "force-stop", "com.tencent.ima"))
            Thread.sleep(2000)
            Runtime.getRuntime().exec(arrayOf("monkey", "-p", "com.tencent.ima", "-c", "android.intent.category.LAUNCHER", "1"))
        } catch (t: Throwable) { log("auto-restart EX " + t) }
    }

    private fun extractSession(txt: String): String {
        if (txt.isBlank()) return ""
        try {
            val o = JSONObject(txt)
            for (k in listOf("session_id", "sessionId", "sessionID")) {
                val v = o.optString(k, ""); if (v.isNotEmpty() && v.length > 8) return v
            }
            val inner = o.optJSONObject("session")
            if (inner != null) for (k in listOf("session_id", "sessionId", "id")) {
                val v = inner.optString(k, ""); if (v.isNotEmpty()) return v
            }
        } catch (_: Throwable) {}
        val m = Regex("session_id[\"':= ]+([0-9a-zA-Z]{20,})").find(txt)
        return m?.groupValues?.get(1) ?: ""
    }

    /** v0.5.1: 扫描所有 scene 分区的 get_models（copilot/问问ima 等），合并去重 */
    private fun fetchModels(): org.json.JSONArray {
        val cached = modelsCache
        if (cached != null && System.currentTimeMillis() - modelsTs < 3600_000L) return cached
        val arr = org.json.JSONArray()
        val seenModels = HashSet<String>() // model_id 去重（跨分区同一模型保留一份）
        return try {
            if (credsNow().isEmpty()) throw IllegalStateException("no creds")
            // scene 枚举探测：1=copilot 分区（已证实），其他值=问问ima 等主界面分区
            for (scene in intArrayOf(1, 2, 3, 4, 0, 5, 6)) {
                try {
                    val txt = postJson(API + "/cgi-bin/model_manage/get_models", JSONObject().put("scene", scene).toString())
                    val before = arr.length()
                    fun walk(o: Any?) {
                        when (o) {
                            is JSONObject -> {
                                if (o.has("model_id")) {
                                    val mid = o.optString("model_id")
                                    if (mid.isNotEmpty() && !seenModels.contains(mid)) {
                                        seenModels.add(mid)
                                        val item = JSONObject()
                                        item.put("model_id", mid)
                                        item.put("model_type", o.optInt("model_type", -1))
                                        val nm = listOf("name", "title", "display_name", "model_name").firstOrNull { o.optString(it).isNotEmpty() }
                                        item.put("name", if (nm != null) o.optString(nm) else mid)
                                        item.put("enable_enhancement", o.optBoolean("enable_enhancement", false))
                                        item.put("scene", scene)
                                        arr.put(item)
                                    }
                                }
                                for (k in o.keys()) walk(o.opt(k))
                            }
                            is org.json.JSONArray -> for (i in 0 until o.length()) walk(o.opt(i))
                            else -> {}
                        }
                    }
                    walk(JSONObject(txt))
                    log("get_models scene=$scene: +" + (arr.length() - before) + " models (resp " + txt.length + "B)")
                } catch (t: Throwable) {
                    log("get_models scene=$scene EX " + t.toString().take(120))
                }
            }
            if (arr.length() > 0) {
                modelsCache = arr; modelsTs = System.currentTimeMillis()
                modelAlias.clear()
                modelAlias["auto"] = Pair("official_100000", 100000)
                for (i in 0 until arr.length()) {
                    val m = arr.optJSONObject(i) ?: continue
                    val nm = m.optString("name"); val mid = m.optString("model_id"); val tp = m.optInt("model_type")
                    if (nm.isNotEmpty() && mid.isNotEmpty() && nm != "自动选择最优模型") modelAlias[nm] = Pair(mid, tp)
                }
                // v0.6: 锚定付费端点，防止上游 get_models 把展示名挂到自动模型
                // 实测 2026-09-06：DeepSeek-V4-Flash 被上游挂到 official_1/type 1，导致静默回退
                val paidAnchors = mapOf(
                    "DeepSeek-V4-Flash" to Pair("official_paid_ep-2ovxbexm", 110000),
                    "DeepSeek-V4-Pro" to Pair("official_paid_ep-11s92or4", 110000)
                )
                for ((nm, meta) in paidAnchors) {
                    val cur = modelAlias[nm]
                    if (cur == null || cur.second != 110000) {
                        modelAlias[nm] = meta
                        log("alias anchor: $nm forced to $meta (upstream gave $cur)")
                    } else {
                        log("alias anchor: $nm already paid endpoint $cur")
                    }
                }
                log("models cached n=" + arr.length() + " alias=" + modelAlias.keys)
            } else {
                return fallbackModels()
            }
            arr
        } catch (t: Throwable) {
            log("get_models EX " + t + " -> fallback hardcoded list")
            fallbackModels()
        }
    }

    /** v0.3: DNS/网络异常时的兜底模型表（2026-09-06 真实 get_models 响应快照） */
    private fun fallbackModels(): org.json.JSONArray {
        val arr = org.json.JSONArray()
        val list = listOf(
            Triple("official_100000", 100000, "自动选择最优模型"),
            Triple("official_paid_hy3", 110000, "Hy3"),
            Triple("official_paid_hy4-preview", 110000, "Hy4 preview"),
            Triple("official_paid_ep-2ovxbexm", 110000, "DeepSeek-V4-Flash"),
            Triple("official_paid_ep-11s92or4", 110000, "DeepSeek-V4-Pro"),
            Triple("official_paid_ep-gn8w57th", 110000, "MiniMax-M2.7"),
            Triple("official_paid_ep-41wf1s9g", 110000, "MiniMax-M3"),
            Triple("official_paid_ep-orngvu1u", 110000, "GLM-5.3-Flash"),
            Triple("official_paid_ep-6mnmwpgi", 110000, "GLM-5.3"),
            Triple("official_paid_ep-iwb7hj0b", 110000, "Kimi-K2.7 Code")
        )
        modelAlias.clear()
        modelAlias["auto"] = Pair("official_100000", 100000)
        for ((id, tp, nm) in list) {
            arr.put(JSONObject().put("model_id", id).put("model_type", tp).put("name", nm).put("enable_enhancement", false))
            if (nm != "自动选择最优模型") modelAlias[nm] = Pair(id, tp)
        }
        return arr
    }

    private fun qaBody(question: String, sid: String, withHistory: Boolean = false, extra: JSONObject? = null): String {
        val body = JSONObject()
        body.put("session_id", sid)
        body.put("robot_type", 10000)
        body.put("question", question)
        body.put("question_type", 1)
        body.put("command_info", JSONObject().put("question_info", JSONObject()))
        body.put("client_id", clientId)
        body.put("model_info", JSONObject().put("model_type", modelType).put("enable_enhancement", enhance).put("model_id", modelId))
        if (withHistory) {
            // v0.3: 让服务端携带会话全部历史（多轮记忆的关键开关）
            body.put("history_type", 0) // HistoryType.ALL_HISTORY = 0 (smali clinit 证实)
            body.put("history_info", JSONObject().put("type", 0).put("message_seq_ids", JSONArray()))
        }
        // v0.4.5: 自定义参数透传（Operit 思考配置等）——不覆盖已生成的标准字段
        if (extra != null) for (k in extra.keys()) if (!body.has(k)) body.put(k, extra.opt(k))
        return body.toString()
    }

    fun askWithSession(question: String, extKey: String?, withHistory: Boolean, onDelta: (String) -> Unit, onThink: (String) -> Unit = {}, extra: JSONObject? = null): Pair<String, String> {
        var sid = if (extKey != null) (sessions[extKey] ?: "") else ""
        var isNew = false
        if (sid.isEmpty()) {
            if (credsNow().isEmpty()) throw IllegalStateException("ima creds not captured yet; open IMA once first")
            sid = initSession(question.take(40))
            if (sid.isEmpty()) throw IllegalStateException("init_session failed: " + lastErr)
            val key = extKey ?: ("s_" + UUID.randomUUID().toString().replace("-", "").take(12))
            sessions[key] = sid
            sessionId = sid
            isNew = true
        }
        // v0.3: 复用的会话带 ALL_HISTORY 让服务端记忆上文；全新会话无历史可带
        log("ask sess=" + sid.take(12) + " isNew=$isNew withHistory=" + (withHistory && !isNew) + " qLen=" + question.length)
        val full = askStream(API + "/cgi-bin/assistant/qa", qaBody(question, sid, withHistory && !isNew, extra), onDelta, onThink)
        return Pair(full, sid)
    }

    // ---------- HTTP server ----------
    private fun serve() {
        var ss: ServerSocket? = null
        try {
            ss = ServerSocket()
            ss.reuseAddress = true
            ss.bind(java.net.InetSocketAddress(InetAddress.getByName("127.0.0.1"), PORT))
            log("ImatGateway listening 127.0.0.1:" + PORT)
        } catch (t: Throwable) { log("bind err: " + t); return }
        while (running.get()) {
            try { val s = ss.accept(); pool.execute { handle(s) } } catch (_: Throwable) { break }
        }
    }

    private fun handle(sock: Socket) {
        try {
            sock.soTimeout = 240000
            // v0.4: 字节级读取（修复 Content-Length 字节数 vs CharArray 字符数不匹配导致中文大 body 卡死）
            val ins = sock.getInputStream()
            fun readLineFrom(ins: InputStream): String {
                val sb = StringBuilder()
                var b = ins.read()
                while (b >= 0) {
                    if (b == 10) break // \n
                    if (b != 13) sb.append(b.toChar()) // 跳过 \r
                    b = ins.read()
                }
                return sb.toString()
            }
            val reqLine = readLineFrom(ins)
            if (reqLine.isEmpty()) return
            val parts = reqLine.split(" ")
            if (parts.size < 2) return
            val method = parts[0]
            val path = parts[1].substringBefore("?")
            var contentLength = 0
            var expect100 = false
            var chunkedReq = false
            while (true) {
                val line = readLineFrom(ins)
                if (line.isEmpty()) break
                val low = line.lowercase()
                if (low.startsWith("content-length:")) contentLength = line.substringAfter(":").trim().toIntOrNull() ?: 0
                if (low.startsWith("expect:") && low.contains("100-continue")) expect100 = true
                if (low.startsWith("transfer-encoding:") && low.contains("chunked")) chunkedReq = true
            }
            val out0 = sock.getOutputStream()
            if (expect100 && contentLength > 0) {
                out0.write("HTTP/1.1 100 Continue\r\n\r\n".toByteArray(Charsets.UTF_8)); out0.flush()
            }
            val body = if (chunkedReq) {
                // v0.3.4: 客户端 body 也可能用 chunked 上传——按 chunk 协议重组（字节级）
                val all = java.io.ByteArrayOutputStream()
                while (true) {
                    val sizeLine = readLineFrom(ins)
                    val sz = sizeLine.trim().toIntOrNull(16) ?: break
                    if (sz == 0) break
                    val chunk = ByteArray(sz); var off = 0
                    while (off < sz) { val r = ins.read(chunk, off, sz - off); if (r < 0) break; off += r }
                    all.write(chunk, 0, off)
                    readLineFrom(ins) // chunk 后的 \r\n
                }
                all.toString(Charsets.UTF_8)
            } else if (contentLength > 0) {
                // ★ 关键修复：按 Content-Length 的【字节数】读原始字节，再 UTF-8 解码
                // （旧版用 CharArray 按【字符数】读——中文 body 字节数>字符数，永远读不够导致卡死）
                val bytes = ByteArray(contentLength); var off = 0
                while (off < contentLength) { val r = ins.read(bytes, off, contentLength - off); if (r < 0) break; off += r }
                String(bytes, 0, off, Charsets.UTF_8)
            } else ""
            log("req $method $path bodyChars=${body.length} contentLen=$contentLength 100c=$expect100 chunkedReq=$chunkedReq")
            when {
                path == "/__status" -> {
                    val o = JSONObject()
                    o.put("gw", "imat-v0.8"); o.put("port", PORT); o.put("sessionId", sessionId)
                    o.put("credTs", credTs); o.put("hasCreds", credsNow().isNotEmpty())
                    o.put("model", JSONObject().put("model_type", modelType).put("model_id", modelId))
                    o.put("sessions", sessions.size); o.put("lastErr", lastErr)
                    write(sock, 200, "application/json", o.toString())
                }
                path == "/__selftest" && method == "POST" -> {
                    try {
                        val o = JSONObject(body)
                        val q = o.optString("question", "1+1=?")
                        val full = askWithSession(q, null, false, { _ -> }).first
                        val resp = JSONObject().put("ok", true).put("answer", full).put("session_id", sessionId).toString()
                        log("selftest answer len=" + full.length + " resp=" + resp.take(200))
                        try { write(sock, 200, "application/json", resp); log("selftest wrote ok") } catch (t: Throwable) { log("selftest write FAIL " + t) }
                    } catch (t: Throwable) { write(sock, 500, "application/json", JSONObject().put("ok", false).put("err", t.toString()).toString()) }
                }
                path == "/__creds" && method == "GET" -> {
                    val o = JSONObject()
                    o.put("ts", credTs)
                    val ks = JSONObject()
                    for ((k, v) in credsNow()) ks.put(k, if (v.length > 12) v.take(8) + "..." + v.takeLast(4) + "(len=" + v.length + ")" else v)
                    o.put("headers", ks)
                    write(sock, 200, "application/json", o.toString())
                }
                path == "/v1/models" && method == "GET" -> {
                    fetchModels() // 刷新别名表（有缓存 1h）
                    val arr = JSONArray()
                    for ((alias, meta) in modelAlias) {
                        arr.put(JSONObject().put("id", alias)
                            .put("object", "model").put("owned_by", "ima")
                            .put("model_id", meta.first).put("model_type", meta.second))
                    }
                    if (arr.length() == 0) arr.put(JSONObject().put("id", modelName).put("object", "model").put("owned_by", "ima"))
                    write(sock, 200, "application/json", JSONObject().put("object", "list").put("data", arr).toString())
                }
                path == "/v1/chat/completions" && method == "POST" -> handleChat(sock, body)
                else -> write(sock, 404, "application/json", "{\"err\":\"not found\"}")
            }
        } catch (t: Throwable) {
            log("handle EX " + t + "\n" + Log.getStackTraceString(t).take(800))
            try { write(sock, 500, "application/json", "{\"err\":\"" + t.toString().replace("\"", "'") + "\"}") } catch (_: Throwable) {}
        } finally { try { sock.close() } catch (_: Throwable) {} }
    }

    private fun handleChat(sock: Socket, body: String) {
        try {
            log("chat begin bodyLen=" + body.length)
            val req = try { JSONObject(body) } catch (t: Throwable) {
                log("chat JSON parse EX: " + t + " | head=" + body.take(200))
                write(sock, 400, "application/json", "{\"err\":\"bad json\"}"); return
            }
            val stream = req.optBoolean("stream", false)
            val msgs = req.optJSONArray("messages") ?: JSONArray()
            log("chat msgs=" + msgs.length() + " stream=" + stream + " model=" + req.optString("model"))

            // v0.3: 解析 OpenAI messages -> system 指令 + 对话轮次
            val systems = StringBuilder()
            val dialog = ArrayList<Pair<String, String>>() // role -> content
            for (i in 0 until msgs.length()) {
                val m = msgs.optJSONObject(i) ?: continue
                val role = m.optString("role")
                val content = when (val c = m.opt("content")) {
                    is String -> c
                    is org.json.JSONArray -> {
                        // 多模态 content 数组：拼接 text 部分
                        val sb = StringBuilder()
                        for (j in 0 until c.length()) {
                            val part = c.optJSONObject(j) ?: continue
                            if (part.optString("type") == "text") sb.append(part.optString("content")).append('\n')
                        }
                        sb.toString()
                    }
                    else -> c?.toString() ?: ""
                }
                when (role) {
                    "system" -> systems.append(content).append('\n')
                    "user", "assistant" -> if (content.isNotEmpty()) dialog.add(Pair(role, content))
                    "tool" -> if (content.isNotEmpty()) dialog.add(Pair("tool", content)) // v0.4.7: 工具结果进上下文
                }
            }
            val lastUser = dialog.lastOrNull { it.first == "user" }?.second ?: ""
            if (lastUser.isEmpty()) { write(sock, 400, "application/json", "{\"err\":\"no user message\"}"); return }

            // v0.9：对齐 Operit 客户端实际解析格式（废弃旧 XML，避免「Unknown tool」）
            val toolsArr = req.optJSONArray("tools")
            if (toolsArr != null && toolsArr.length() > 0) {
                systems.append("\n[可用工具与调用格式·Operit 协议]\n")
                systems.append("需要调用工具时，必须严格使用以下 Operit 原生 XML 属性格式输出（禁止代码块包裹、禁止附加任何解释文字）：\n")
                systems.append("<tool name=\"工具名\">\n<param name=\"参数名\">参数值</param>\n...\n</tool>\n")
                systems.append("硬性要求：\n")
                systems.append("1. 工具元素必须是 <tool name=\"...\">，参数用 <param name=\"...\">值</param> 子元素；参数值直接放在标签内，不要用 JSON。\n")
                systems.append("2. 严禁在参数值或标签内出现 <parameter>、<tool_call>、JSON 花括号等其它格式。\n")
                systems.append("3. 一次只能输出一个 <tool>，多个参数写多个 <param>。\n")
                systems.append("4. 输出 </tool> 后立即停止，禁止附加任何文字。\n")
                systems.append("5. 工具调用必须放在【/思考】标记之后、正文区域，严禁混在思考内容里。\n")
                for (i in 0 until toolsArr.length()) {
                    val t = toolsArr.getJSONObject(i)
                    val fn = t.optString("name"); val desc = t.optString("description", "")
                    systems.append("· $fn：$desc\n")
                    val props = t.optJSONObject("parameters")?.optJSONObject("properties")
                    if (props != null) for (pk in props.keys()) {
                        val pd = props.optJSONObject(pk)
                        val reqd = t.optJSONObject("parameters")?.optJSONArray("required")
                        val isReq = reqd != null && (0 until reqd.length()).any { reqd.optString(it) == pk }
                        systems.append("  - $pk (${pd?.optString("type", "string")}${if (isReq) ",必填" else ""})：${pd?.optString("description", "")}\n")
                    }
                }
                log("tools injected n=" + toolsArr.length())

            }

            // v0.8.2 深度思考协议（无条件注入；有工具时自动互斥）
            run {
                val tg = StringBuilder()
                tg.append("\n[深度思考协议 v3（Kimi 增强版）]\n")
                tg.append("回答任何纯文本问题前，先用【思考】标记开头，按三阶推进并显式呈现思考过程：\n")
                tg.append("1. 意图拆解：拆解用户真实目标、约束与隐含需求；\n")
                tg.append("2. 步骤推进：列出关键步骤与依赖，原子化推进；\n")
                tg.append("3. 自我反思：对前序判断至少找出一处盲点并修正。\n")
                tg.append("推理要深入、诚实，不确定处明确标注；思考篇幅至少 80 字，禁止只写一句话。\n")
                tg.append("再用【/思考】标记结束，然后另起一行给出最终回答。\n")
                tg.append("标记必须成对、逐字使用方头括号，不要用 XML 或其他变体；思考内容内部不要再出现这两个标记字样。\n")
                if (toolsArr != null && toolsArr.length() > 0) {
                    tg.append("如果你需要调用工具，也必须先完成【思考】三阶推理再输出工具调用 XML；思考与工具调用可以共存，思考结束后另起一行直接输出工具 XML。")
                }
                systems.append(tg)
            }

            // 会话策略：
            //  A. 显式 session 参数 -> 服务端记忆模式（history_type=ALL_HISTORY，question 只发当前问题）
            //  B. 无 session -> 无状态模式（system + 全部历史轮次拼进 question，每次新会话）
            val explicitSession = req.optString("session", "")
            val extKey: String?
            val withHistory: Boolean
            val question: String
            // v0.8.9-fix: 当前问题用强强调标记包裹，防止长上下文注意力稀释导致「读了但没读进去」
            // （会话模式与无状态模式共用同一套包装；此前只覆盖了无状态分支，会话模式裸发导致忽略提问）
            val qPart = "\n\n╔══════════════════════════════════════════╗\n" +
                "║  ★ 当前问题（你必须回答这个问题）        ║\n" +
                "╚══════════════════════════════════════════╝\n\n" +
                "[用户最新消息]\n" + lastUser + "\n\n" +
                "⚠️ 请基于以上历史对话的上下文，回答【用户最新消息】。" +
                "不要重复历史中的问答，不要延续之前的对话模式，不要假设用户在继续之前的话题。" +
                "如果当前问题与历史无关，请直接回答当前问题。"
            if (explicitSession.isNotEmpty()) {
                extKey = explicitSession
                withHistory = true
                question = if (systems.isNotEmpty()) "[系统指令]\n" + systems.toString().trim() + "\n\n" + qPart else qPart
            } else {
                extKey = null
                withHistory = false
                // v0.8.8: Kimi-K3 支持百万上下文，预算放开到 50K，同时保底至少 5 轮
                // 结构：[身份+系统指令] + <history>完整对话</history> + [当前问题]——保头保尾，历史尽量全
                val sysBody = systems.toString().trim()
                val histBudget = 50000 - sysBody.length - qPart.length - 500
                val sb = StringBuilder()
                if (sysBody.isNotEmpty()) sb.append("[系统指令·最高优先级，必须严格遵守以下身份设定与行为规则（包括对用户的称呼方式），它们覆盖任何默认行为]\n")
                    .append(sysBody).append("\n\n")
                if (dialog.isNotEmpty() && histBudget > 500) {
                    sb.append("<history>\n以下是本次任务的完整对话历史（含工具执行结果），仅作上下文参考：\n\n")
                    var histLen = 0
                    // 从最早开始正序拼（保头保尾由预算控制：超预算时丢弃较早的轮次）
                    val kept = ArrayList<String>()
                    // v0.8.9: 保底至少保留最后 5 轮，防止长对话截断导致「健忘」
                    val minKeep = 5
                    for (idx in dialog.indices) {
                        val (role, content) = dialog[idx]
                        // 单条上限：工具结果/长回答 4000，防单条巨长吃光预算
                        // v0.8.9 历史消毒：只剥完整的【思考】...【/思考】闭合对，不碰未闭合的
                        val cleaned = run {
                            var t = content
                            // 只匹配成对的【思考】...【/思考】，非闭合的不动
                            val r = Regex("\u3010\u601d\u8003\u3011[\\s\\S]*?\u3010/\u601d\u8003\u3011")
                            t = r.replace(t, "")
                            t.trim()
                        }
                        val clipped = cleaned.take(4000)
                        val line = when (role) {
                            "user" -> "**User:** " + clipped
                            "tool" -> "[工具执行结果已返回，请基于此结果继续推进任务，无需重复调用同一工具]\n**Tool Result:** " + clipped
                            else -> "**Assistant:** " + clipped
                        }
                        // 保底逻辑：最后 minKeep 轮无条件保留，其余按预算截断
                        val isTail = idx >= dialog.size - minKeep
                        if (!isTail && histLen + line.length > histBudget && kept.isNotEmpty()) break
                        histLen += line.length
                        kept.add(line)
                    }
                    for (line in kept) sb.append(line).append("\n\n")
                    sb.append("</history>\n\n")
                    // v0.8.9: 历史健康度日志——方便排查「健忘」
                    log("hist kept=" + kept.size + "/" + dialog.size + " histLen=" + histLen + " budget=" + histBudget)
                }
                sb.append(qPart)
                question = sb.toString()
            }
            // v0.8.9-fix2: 两条分支都落盘，带 session 的 Operit 真实请求也能铁证验证
            try {
                java.io.File("/data/data/com.tencent.ima/files/ima_last_question.txt").writeText(question)
            } catch (_: Throwable) {}

            if (req.has("model")) {
                val want = req.optString("model")
                val hit = modelAlias[want]
                if (hit != null) {
                    modelId = hit.first; modelType = hit.second; modelName = want
                } else if (want.isNotEmpty()) {
                    modelName = want // 未知模型名：保持默认 3000 组
                }
            }
            if (req.has("enhance")) enhance = req.optBoolean("enhance")
            if (req.has("model_type")) modelType = req.optInt("model_type")
            // v0.4.5: 自定义参数透传（Operit 思考配置等）——标准字段之外的都进 extra
            val extra = JSONObject()
            val std = setOf("model", "stream", "messages", "session", "enhance", "model_type", "tools", "tool_choice")
            for (k in req.keys()) if (k !in std) extra.put(k, req.opt(k))
            if (stream) {
                // v0.7: 真流式透传——思考逐片直发，正文做「标签感知缓冲」
                // （避免把 <tool name=...> 从中间切开；不在标签内时按换行/24字符分片即时 flush）
                val out = sock.getOutputStream()
                val head = "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: text/event-stream\r\n" +
                    "Cache-Control: no-cache\r\n" +
                    "Access-Control-Allow-Origin: *\r\n" +
                    "Transfer-Encoding: chunked\r\n\r\n"
                out.write(head.toByteArray(Charsets.ISO_8859_1)); out.flush()
                val id = "chatcmpl-" + UUID.randomUUID().toString().replace("-", "").take(16)
                val created = System.currentTimeMillis() / 1000
                fun writeChunk(data: String) {
                    val bytes = data.toByteArray(Charsets.UTF_8)
                    out.write(Integer.toHexString(bytes.size).toByteArray(Charsets.ISO_8859_1))
                    out.write("\r\n".toByteArray(Charsets.ISO_8859_1))
                    out.write(bytes)
                    out.write("\r\n".toByteArray(Charsets.ISO_8859_1))
                    out.flush()
                }
                fun sseData(json: String) = "data: $json\n\n"
                fun chunkJson(deltaJson: String, fr: String?): String =
                    JSONObject().apply {
                        put("id", id); put("object", "chat.completion.chunk"); put("created", created); put("model", modelName)
                        put("choices", JSONArray().put(JSONObject().apply {
                            put("index", 0)
                            put("delta", JSONObject(deltaJson))
                            if (fr != null) put("finish_reason", fr)
                        }))
                    }.toString()
                writeChunk(sseData(chunkJson("{\"role\":\"assistant\"}", null)))
                var thinkChars = 0
                fun emitThink(piece: String) {
                    if (piece.isEmpty()) return
                    thinkChars += piece.length
                    dumpFrame("stream", "think:" + piece)  // v0.8.4 透视镜：思考帧落盘
                    writeChunk(sseData(chunkJson("{\"reasoning_content\":${JSONObject.quote(piece)}}", null)))
                }
                // v0.8: 思考标记扫描状态
                var inThink = false
                val thinkBuf = StringBuilder()
                val TKO = "【思考】"
                val TKC = "【/思考】"
                fun flushThink() {
                    if (thinkBuf.isNotEmpty()) {
                        emitThink(thinkBuf.toString())
                        thinkBuf.setLength(0)
                    }
                }
                val pending = StringBuilder()
                val scanBuf = StringBuilder()  // v0.8.6: 跨碎片滚动扫描缓冲（标记可跨流式碎片）
                var contentChars = 0
                fun flushPending() {
                    if (pending.isEmpty()) return
                    val s = pending.toString()
                    pending.setLength(0)
                    contentChars += s.length
                    dumpFrame("stream", "content:" + s)  // v0.8.4 透视镜：正文帧落盘
                    writeChunk(sseData(chunkJson("{\"content\":${JSONObject.quote(s)}}", null)))
                }
                fun emitContent(piece: String) {
                    if (piece.isEmpty()) return
                    // v0.8.6: 跨碎片滚动扫描状态机。标记可被拆成多个流式碎片，
                    // 用 scanBuf 累积再扫描，避免 indexOf 在单碎片内找不到完整标记。
                    scanBuf.append(piece)
                    var pos = 0
                    val text = scanBuf.toString()
                    while (pos < text.length) {
                        if (inThink) {
                            val i2 = text.indexOf(TKC, pos)
                            if (i2 >= 0) {
                                if (i2 > pos) thinkBuf.append(text.substring(pos, i2))
                                pos = i2 + TKC.length
                                flushThink()
                                inThink = false
                            } else {
                                val keepLen = minOf(TKC.length - 1, text.length - pos)
                                if (text.length - pos > keepLen) {
                                    thinkBuf.append(text.substring(pos, text.length - keepLen))
                                }
                                scanBuf.setLength(0)
                                scanBuf.append(text.substring(text.length - keepLen))
                                break
                            }
                        } else {
                            val i2 = text.indexOf(TKO, pos)
                            if (i2 >= 0) {
                                if (i2 > pos) pending.append(text.substring(pos, i2))
                                pos = i2 + TKO.length
                                inThink = true
                            } else {
                                val keepLen = minOf(TKO.length - 1, text.length - pos)
                                if (text.length - pos > keepLen) {
                                    pending.append(text.substring(pos, text.length - keepLen))
                                }
                                scanBuf.setLength(0)
                                scanBuf.append(text.substring(text.length - keepLen))
                                break
                            }
                        }
                    }
                    if (!inThink) {
                        val s2 = pending.toString()
                        if (s2.contains("\n") || s2.length >= 24) flushPending()
                    }
                }
                flushThink()  // v0.8: residual think flush
                askWithSession(question, extKey, withHistory,
                    onDelta = { piece -> emitContent(piece) },
                    onThink = { piece -> emitThink(piece) },
                    extra = extra
                )
                // v0.8.6: 冲刷扫描残尾（跨碎片标记的尾巴可能留在 scanBuf）
                if (scanBuf.isNotEmpty()) {
                    if (inThink) thinkBuf.append(scanBuf.toString()) else pending.append(scanBuf.toString())
                    scanBuf.setLength(0)
                }
                flushThink()
                flushPending()
                writeChunk(sseData(chunkJson("{}", "stop")))
                writeChunk("data: [DONE]\n\n")
                out.write("0\r\n\r\n".toByteArray(Charsets.ISO_8859_1))
                out.flush()
                log("chat streamed thinkChars=" + thinkChars + " contentChars=" + contentChars)
                out.close()
            } else {
                val thinkBuf = StringBuilder()
                var full = askWithSession(question, extKey, withHistory, { _ -> }, { thinkBuf.append(it) }, extra).first
                dumpFrame("nostream", full)  // v0.8.4 透视镜：非流式全文落盘
                // v0.8: 非流式思考重铸（方头括号标记 -> reasoning_content）
                run {
                    val tko = "【思考】"; val tkc = "【/思考】"
                    while (true) {
                        val a = full.indexOf(tko)
                        if (a < 0) break
                        val b = full.indexOf(tkc, a)
                        if (b < 0) { thinkBuf.append(full.substring(a + tko.length)); full = full.substring(0, a); break }
                        thinkBuf.append(full.substring(a + tko.length, b))
                        full = full.substring(0, a) + full.substring(b + tkc.length)
                    }
                }
                val id = "chatcmpl-" + UUID.randomUUID().toString().replace("-", "").take(16)
                val o = JSONObject()
                o.put("id", id); o.put("object", "chat.completion"); o.put("created", System.currentTimeMillis() / 1000); o.put("model", modelName)
                val c = JSONObject()
                c.put("index", 0)
                val msgObj = JSONObject().put("role", "assistant").put("content", full)
                if (thinkBuf.isNotEmpty()) msgObj.put("reasoning_content", thinkBuf.toString())
                c.put("message", msgObj)
                c.put("finish_reason", "stop")
                o.put("choices", JSONArray().put(c))
                o.put("usage", JSONObject().put("prompt_tokens", 0).put("completion_tokens", full.length).put("total_tokens", full.length))
                o.put("session_id", sessionId)
                write(sock, 200, "application/json", o.toString())
            }
        } catch (t: Throwable) {
            log("chat EX " + t + "\n" + Log.getStackTraceString(t).take(1200))
            try { write(sock, 500, "application/json", "{\"err\":\"" + t.toString().replace("\"", "'") + "\"}") } catch (_: Throwable) {}
        }
    }

    // v0.8.4 输出透视镜：把发给 Operit 的原始帧落盘，用于定位 `>>` 乱码根因
    private fun dumpFrame(kind: String, data: String) {
        try {
            val dir = java.io.File("/data/data/com.tencent.ima/files/imat_debug")
            if (!dir.exists()) dir.mkdirs()
            val f = java.io.File(dir, if (kind == "stream") "last_stream_frames.txt" else "last_nostream.txt")
            f.appendText("[" + System.currentTimeMillis() + "] " + data.replace("\n", "\\n") + "\n")
        } catch (_: Throwable) {}
    }

    private fun write(sock: Socket, code: Int, type: String, body: String) {
        val out: OutputStream = sock.getOutputStream()
        val reason = when (code) { 200 -> "OK"; 404 -> "Not Found"; 400 -> "Bad Request"; 500 -> "Error"; else -> "OK" }
        val len = body.toByteArray(Charsets.UTF_8).size
        val head = "HTTP/1.1 " + code + " " + reason + "\r\nContent-Type: " + type + "\r\nContent-Length: " + len + "\r\nConnection: close\r\n\r\n"
        out.write(head.toByteArray(Charsets.UTF_8))
        out.write(body.toByteArray(Charsets.UTF_8)); out.flush(); out.close()
    }
}