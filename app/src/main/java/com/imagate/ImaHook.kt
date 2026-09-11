package com.imagate

import de.robv.android.xposed.XC_MethodHook
import de.robv.android.xposed.XposedBridge
import de.robv.android.xposed.XposedHelpers
import de.robv.android.xposed.callbacks.XC_LoadPackage
import java.io.File
import java.io.FileWriter

/**
 * ImaHook v2 (diag) - PC-side findings 2026-09-05
 * Main chat (QaRepository im/x) -> SSE plaintext, NOT kw/b.d.
 * Capture to /data/data/com.tencent.ima/files/ima_hook_capture.log + XposedBridge.log
 */
object ImaHook {

    const val TARGET_PKG = "com.tencent.ima"
    private const val CAP = "/data/data/com.tencent.ima/files/ima_hook_capture.log"

    fun hook(lpparam: XC_LoadPackage.LoadPackageParam) {
        val cl = lpparam.classLoader
        log("=== ImaHook v2 inject ===")
        hookKwB(cl)
        hookJwL(cl)
        hookLrP(cl)
        hookImX(cl)
        hookSseEvents(cl)
        hookOutbound(cl)
        hookJwKNew(cl)
        log("ImaHook v2 installed: kw.b / jw.k(new) / jw.l / lr.p / im.x / jw.j|jw.k")
        // 栈探测：hook 底层输出流，打印调用栈（定位 IMA 新版网络层的通用手段）
        try {
            val os = XposedHelpers.findClass("java.io.OutputStream", cl)
            XposedBridge.hookAllMethods(os, "write", object : XC_MethodHook() {
                private var last = 0L
                override fun beforeHookedMethod(p: MethodHookParam) {
                    val now = System.currentTimeMillis()
                    if (now - last < 2500) return
                    val st = Thread.currentThread().stackTrace
                    val frames = st.take(40)
                    val joined = frames.joinToString(" <- ") { it.className.substringAfterLast('.') + "." + it.methodName }
                    val low = joined.lowercase()
                    if (low.contains("okhttp") || low.contains("retrofit") || low.contains("http") || low.contains("socket")) {
                        last = now
                        mark("STACK-PROBE", head(joined, 1800))
                    }
                }
            })
            log("stack probe installed")
        } catch (t: Throwable) { log("stack probe err: " + t) }

        // 探测（定位新版 IMA 的 HTTP 出入口）：jw.a~jw.l + kw.a~kw.b 全挂
        for (suffix in listOf("a","b","c","d","e","f","g","h","i","j","k","l")) probeAll(cl, "jw.$suffix", "jw.$suffix")
        for (suffix in listOf("a","b")) probeAll(cl, "kw.$suffix", "kw.$suffix")
        try {
            ImGate.logToXposed = { s -> XposedBridge.log("[ImaHook] " + s) }
            ImGate.loadCredsFromDisk()
            ImGate.start(cl)
            log("ImGate started")
        } catch (t: Throwable) { log("ImGate start err: " + t) }
    }

    // v1.7.1j 探测模式：hook 目标类的所有方法重载并打印签名（IMA 更新后定位新入口用）
    private fun probeAll(cl: ClassLoader, cls: String, tag: String) {
        try {
            val c = XposedHelpers.findClass(cls, cl)
            val ms = c.declaredMethods
            for (m in ms) {
                if (m.isSynthetic) continue
                try {
                    XposedBridge.hookMethod(m, object : XC_MethodHook() {
                        override fun beforeHookedMethod(p: MethodHookParam) {
                            val sig = p.args.mapIndexed { i, a -> i.toString() + ":" + (a?.javaClass?.simpleName ?: "null") }.joinToString(",")
                            var hit = false
                            for (a in p.args) {
                                val s = a?.toString() ?: ""
                                if (s.contains("ima.qq.com") || s.contains("x-ima-cookie") || s.contains("cgi-bin")) hit = true
                            }
                            if (hit) mark("PROBE $tag", cls + "." + m.name + " argc=" + p.args.size + " [" + sig + "] head=" + head(p.args.firstOrNull { it?.toString()?.contains("ima.qq.com") == true }?.toString() ?: p.args.firstOrNull()?.toString() ?: "", 200))
                        }
                    })
                } catch (_: Throwable) {}
            }
            log("probe installed on $cls methods=" + ms.size)
        } catch (t: Throwable) { log("probe $cls err: " + t) }
    }

    private fun hookKwB(cl: ClassLoader) {
        try {
            val c = XposedHelpers.findClass("kw.b", cl)
            hookMethods(c, "d", intArrayOf(1)) { p, kind ->
                if (kind == 0) {
                    val pt = p.args[0] as? ByteArray
                    if (pt != null) mark("kw/b.d encrypt-before", "plaintext_len=" + pt.size + " hex=" + pt.toHex(96))
                } else {
                    val r = p.result ?: return@hookMethods
                    val body = XposedHelpers.getObjectField(r, "a") as? String
                    val mode = XposedHelpers.getIntField(r, "b")
                    val hdrs = XposedHelpers.getObjectField(r, "c")
                    mark("kw/b.d encrypt-after", "mode=" + mode + " bodyLen=" + (body?.length ?: -1) + " headers=" + head(hdrs?.toString() ?: "", 400))
                    // v1.7.1j: IMA 更新后 tg0.i0 消失，改从 kw.b.d 的 headers 字段抽取凭证（x-ima-cookie/bkn/referer/origin）
                    try {
                        val cm = LinkedHashMap<String, String>()
                        if (hdrs is Map<*, *>) {
                            for ((k, v) in hdrs) {
                                val nm = k?.toString() ?: ""
                                if (nm.startsWith("x-ima-") || nm == "from_browser_ima" || nm == "referer" || nm == "origin" || nm.equals("Content-Type", true)) {
                                    cm[nm] = v?.toString() ?: ""
                                }
                            }
                        } else {
                            val hs = hdrs?.toString() ?: ""
                            if (hs.contains("x-ima-cookie")) {
                                for (pair in hs.trim('{', '}').split(", ")) {
                                    val ix = pair.indexOf('=')
                                    if (ix <= 0) continue
                                    val nm = pair.substring(0, ix).trim()
                                    if (nm.startsWith("x-ima-") || nm == "from_browser_ima" || nm == "referer" || nm == "origin" || nm.equals("Content-Type", true)) {
                                        cm[nm] = pair.substring(ix + 1).trim()
                                    }
                                }
                            }
                        }
                        if (cm.isNotEmpty()) {
                            ImGate.updateCreds(cm)
                            mark("CREDS-FROM-KWB", cm.keys.joinToString(","))
                        }
                    } catch (t: Throwable) { mark("kwb creds err", t.toString()) }
                }
            }
            log("hook kw.b ok")
        } catch (t: Throwable) { log("hookKwB err: " + t) }
    }

    private fun hookJwL(cl: ClassLoader) {
        try {
            val c = XposedHelpers.findClass("jw.l", cl)
            hookMethods(c, "c", intArrayOf(6)) { p, _ ->
                val body = p.args[0] as? String
                val url = p.args[1] as? String
                mark("jw/l.c SSE-connect", "url=" + url + " bodyLen=" + (body?.length ?: -1) + " bodyHead=" + head(body ?: "", 800))
            }
            hookMethods(c, "a", intArrayOf(6)) { p, kind ->
                val b = p.args[0]
                val url = p.args[1] as? String
                if (kind == 0) {
                    mark("jw/l.a HTTP-POST", "url=" + url + " bodyType=" + (b?.javaClass?.name ?: "") + " bodyHead=" + head(b?.toString() ?: "", 300))
                    // v1.4.1 刀一诊断2: jw/l.a 全参数签名（反射调用姿势）+ create_media 调用栈
                    if (url?.contains("create_media") == true) {
                        val sig = p.args.mapIndexed { i, a -> "arg$i=" + (a?.javaClass?.name ?: "null") + ":" + head(a?.toString() ?: "", 80) }.joinToString(" | ")
                        mark("jw/l.a ARGS", sig)
                        val frames = Thread.currentThread().stackTrace.drop(4).take(16)
                            .joinToString(" <- ") { it.className.substringAfterLast('.') + "." + it.methodName + ":" + it.lineNumber }
                        mark("jw/l.a CALLSTACK", frames)
                    }
                } else if (url?.contains("create_media") == true) {
                    // v1.4.1 刀一诊断2: xg0.j 响应字段全量展开（media_id / COS 预签名凭证形态）
                    val r = p.result
                    if (r == null) { mark("jw/l.a RESPONSE", "null") } else {
                        val sb = StringBuilder()
                        try {
                            var cls: Class<*>? = r.javaClass
                            var depth = 0
                            while (cls != null && depth < 3) {
                                for (f in cls.declaredFields) {
                                    if (java.lang.reflect.Modifier.isStatic(f.modifiers)) continue
                                    try {
                                        f.isAccessible = true
                                        val v = f.get(r)
                                        sb.append(cls.simpleName).append('.').append(f.name).append('=').append(head(v?.toString() ?: "null", 500)).append(" || ")
                                    } catch (_: Throwable) {}
                                }
                                cls = cls.superclass; depth++
                            }
                        } catch (_: Throwable) {}
                        mark("jw/l.a RESPONSE", "cls=" + r.javaClass.name + " FIELDS: " + head(sb.toString(), 1600))
                    }
                }
            }
            hookMethods(c, "f", intArrayOf(3)) { p, _ ->
                val body = p.args[0] as? String
                val url = p.args[1] as? String
                mark("jw/l.f encrypt-POST", "url=" + url + " bodyHead=" + head(body ?: "", 300))
            }
            log("hook jw.l ok")
        } catch (t: Throwable) { log("hookJwL err: " + t) }
    }

    // v1.7.1j 新版适配：IMA 2026-09-09 更新后 HTTP 出入口从 jw.l.a 迁移到 jw.k.a
    // （签名 6 参数：[0]=body String, [1]=url String, [2]=LinkedHashMap headers, [3]=Boolean, [4]=r, [5]=null）
    private fun hookJwKNew(cl: ClassLoader) {
        try {
            val c = XposedHelpers.findClass("jw.k", cl)
            hookMethods(c, "a", intArrayOf(6)) { p, kind ->
                if (kind == 1) {
                    val url = p.args[1] as? String
                    val hdrs = p.args[2] as? Map<*, *>
                    if (url != null && url.contains("ima.qq.com") && hdrs != null) {
                        val cm = LinkedHashMap<String, String>()
                        for ((k, v) in hdrs) {
                            val nm = k?.toString() ?: ""
                            if (nm.startsWith("x-ima-") || nm == "from_browser_ima" || nm == "referer" || nm == "origin" || nm.equals("Content-Type", true)) {
                                cm[nm] = v?.toString() ?: ""
                            }
                        }
                        if (cm.isNotEmpty()) {
                            ImGate.updateCreds(cm)
                            mark("CREDS-FROM-JWK", cm.keys.joinToString(","))
                        }
                    }
                }
            }
            log("hook jw.k.a v2 ok")
        } catch (t: Throwable) { log("hook jw.k.a err: " + t) }
    }

    private fun hookLrP(cl: ClassLoader) {
        try {
            val c = XposedHelpers.findClass("lr.p", cl)
            hookMethods(c, "a", intArrayOf(2)) { p, _ -> logPostReq(p.args[0], "lr/p.a encrypt-protobuf-POST") }
            hookMethods(c, "e", intArrayOf(2)) { p, _ -> logPostReq(p.args[0], "lr/p.e plain-protobuf-POST") }
            hookMethods(c, "c", intArrayOf(2)) { p, _ ->
                val req = p.args[0] ?: return@hookMethods
                val url = XposedHelpers.getObjectField(req, "a") as? String
                val data = XposedHelpers.getObjectField(req, "b") as? String
                mark("lr/p.c SSE-request", "url=" + url + " dataHead=" + head(data ?: "", 300))
            }
            hookMethods(c, "g", intArrayOf(3)) { p, _ -> logPostReq(p.args[0], "lr/p.g encrypt-POST(retry)") }
            log("hook lr.p ok")
        } catch (t: Throwable) { log("hookLrP err: " + t) }
    }

    private fun logPostReq(reqObj: Any?, tag: String) {
        if (reqObj == null) return
        try {
            val body = XposedHelpers.getObjectField(reqObj, "a") as? ByteArray
            val url = XposedHelpers.getObjectField(reqObj, "b") as? String
            mark(tag, "url=" + url + " protoLen=" + (body?.size ?: -1) + " protoHex=" + head(body?.toHex(80) ?: "", 200))
        } catch (_: Throwable) {}
    }

    private fun hookImX(cl: ClassLoader) {
        try {
            val c = XposedHelpers.findClass("im.x", cl)
            hookMethods(c, "l", intArrayOf(4)) { p, kind ->
                if (kind == 0) {
                    val b = p.args[0]
                    mark("im/x.l get_stream-SSE", "req=" + (b?.javaClass?.name ?: "") + " content=" + head(b?.toString() ?: "", 1500))
                } else mark("im/x.l -> EventSource", "es=" + (p.result?.javaClass?.name ?: ""))
            }
            hookMethods(c, "o", intArrayOf(4)) { p, kind ->
                if (kind == 0) {
                    val b = p.args[0]
                    mark("im/x.o qa-SSE", "req=" + (b?.javaClass?.name ?: "") + " content=" + head(b?.toString() ?: "", 1500))
                } else mark("im/x.o -> EventSource", "es=" + (p.result?.javaClass?.name ?: ""))
            }
            log("hook im.x ok")
        } catch (t: Throwable) { log("hookImX err: " + t) }
    }

    private fun hookSseEvents(cl: ClassLoader) {
        for (cls in listOf("jw.j", "jw.k")) {
            try {
                val c = XposedHelpers.findClass(cls, cl)
                hookMethods(c, "A", intArrayOf(4)) { p, _ ->
                    val id = p.args[1] as? String
                    val type = p.args[2] as? String
                    val data = p.args[3] as? String
                    mark(cls + " SSE onEvent", "id=" + id + " type=" + type + " dataHead=" + head(data ?: "", 1200))
                }
                hookMethods(c, "B", intArrayOf(3)) { p, _ ->
                    mark(cls + " SSE onFailure", "err=" + head((p.args[1] as? Throwable)?.toString() ?: "", 300))
                }
                hookMethods(c, "C", intArrayOf(2)) { p, _ -> mark(cls + " SSE onClosed", "") }
                hookMethods(c, "z", intArrayOf(1)) { p, _ -> mark(cls + " SSE onOpen", "") }
                log("hook " + cls + " SSE ok")
            } catch (t: Throwable) { log("hookSseEvents(" + cls + ") err: " + t) }
        }
    }

    private fun hookMethods(clazz: Class<*>, name: String, wantParamCounts: IntArray, cb: (XC_MethodHook.MethodHookParam, Int) -> Unit) {
        var hooked = 0
        for (m in clazz.declaredMethods) {
            if (m.name != name) continue
            if (wantParamCounts.isNotEmpty() && !wantParamCounts.contains(m.parameterCount)) continue
            XposedBridge.hookMethod(m, object : XC_MethodHook() {
                override fun beforeHookedMethod(param: XC_MethodHook.MethodHookParam) { try { cb(param, 0) } catch (_: Throwable) {} }
                override fun afterHookedMethod(param: XC_MethodHook.MethodHookParam) { try { cb(param, 1) } catch (_: Throwable) {} }
            })
            hooked++
        }
        log("hooked " + name + " x" + hooked + " on " + clazz.name)
    }

    private fun mark(ev: String, detail: String) {
        val stack = Throwable().stackTrace
        val caller = stack.firstOrNull { it.className != "com.master.qwengate.ImaHook" && !it.className.startsWith("de.robv") }
        val who = if (caller != null) (caller.className.substringAfterLast('.') + "." + caller.methodName) else "?"
        val line = System.currentTimeMillis().toString() + " | " + ev + " | " + detail + " | @" + who
        log(line)
        appendCapture(line)
    }

    // v1.7.1j: 多候选出站 hook（IMA 2026-09-09 更新后 tg0.i0 类名变化，导致凭证捕获失效→网关 500）
    private fun hookOutbound(cl: ClassLoader) {
        val handle: (Any?) -> Unit = { reqObj ->
            val s = reqObj?.toString() ?: ""
            if (s.contains("ima.qq.com/cgi-bin/") && s.contains("x-ima-cookie")) {
                val map = parseHeaders(s)
                if (map.isNotEmpty()) {
                    ImGate.updateCreds(map)
                    if (s.contains("assistant/") || s.contains("session_logic") || s.contains("model_manage") || s.contains("customize_models")) {
                        mark("OUT-REQ-HEADERS", map.keys.joinToString(","))
                    }
                }
            }
        }
        // 候选 1: 旧版混淆类 tg0.i0.b(Request)
        try {
            val c = XposedHelpers.findClass("tg0.i0", cl)
            hookMethods(c, "b", intArrayOf(1)) { p, kind -> if (kind == 1) handle(p.args[0]) }
            log("hook tg0.i0 ok")
        } catch (t: Throwable) { log("hook tg0.i0 err: " + t) }
        // 候选 2: 标准 okhttp3（未 relocate 时稳定，不受 IMA 混淆影响）
        try {
            val c = XposedHelpers.findClass("okhttp3.OkHttpClient", cl)
            hookMethods(c, "newCall", intArrayOf(1)) { p, kind -> if (kind == 1) handle(p.args[0]) }
            log("hook okhttp3.OkHttpClient.newCall ok")
        } catch (t: Throwable) { log("hook okhttp3 err: " + t) }
        // 候选 3: okhttp3 内部 RealCall（兜底，Request 在构造后可从字段取）
        try {
            val c = XposedHelpers.findClass("okhttp3.internal.connection.RealCall", cl)
            hookMethods(c, "execute", intArrayOf(0)) { p, kind -> if (kind == 1) handle(p.thisObject) }
            log("hook RealCall ok")
        } catch (t: Throwable) { log("hook RealCall err: " + t) }
    }

    /** parse okhttp Request.toString headers block -> map */
    private fun parseHeaders(s: String): Map<String, String> {
        try {
            val hs = s.indexOf("headers=[")
            if (hs < 0) return emptyMap()
            val he = s.indexOf("], tags=", hs)
            val block = s.substring(hs + 9, if (he < 0) s.length else he)
            val out = LinkedHashMap<String, String>()
            for (pair in block.split(", ")) {
                val idx = pair.indexOf(':')
                if (idx <= 0) continue
                val name = pair.substring(0, idx).trim()
                val value = pair.substring(idx + 1).trim()
                if (name.startsWith("x-ima-") || name == "from_browser_ima" || name == "referer" || name == "origin" || name.equals("Content-Type", true)) {
                    out[name] = value
                }
            }
            return out
        } catch (_: Throwable) { return emptyMap() }
    }

    private fun head(s: String, n: Int): String = if (s.length <= n) s else s.substring(0, n) + "...(" + s.length + ")"

    private fun log(s: String) = XposedBridge.log("[ImaHook] " + s)

    @Synchronized
    private fun appendCapture(s: String) {
        try {
            val f = File(CAP)
            f.parentFile?.mkdirs()
            FileWriter(f, true).use { it.append(s).append("\n") }
        } catch (_: Throwable) {}
    }

    private fun ByteArray.toHex(n: Int = 64): String {
        val sb = StringBuilder()
        val len = minOf(size, n)
        for (i in 0 until len) sb.append(String.format("%02x", this[i].toInt() and 0xff))
        if (size > len) sb.append("..(+").append(size - len).append(")")
        return sb.toString()
    }
}
