package com.imagate

import de.robv.android.xposed.IXposedHookLoadPackage
import de.robv.android.xposed.XposedBridge
import de.robv.android.xposed.callbacks.XC_LoadPackage

/** ImaGate Xposed 入口：仅注入 com.tencent.ima */
class GateEntry : IXposedHookLoadPackage {
    override fun handleLoadPackage(lpparam: XC_LoadPackage.LoadPackageParam) {
        if (lpparam.packageName == ImaHook.TARGET_PKG) {
            try {
                XposedBridge.log("[ImaGate] injecting into " + lpparam.packageName)
                ImaHook.hook(lpparam)
            } catch (t: Throwable) {
                XposedBridge.log("[ImaGate] inject err: " + t)
            }
        }
    }
}
