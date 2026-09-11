# ImaGate

一个轻量的 Android Xposed 模块框架。不吹不黑，就是自己平时折腾用的那套东西，整理干净了拿出来见人。

## 这玩意儿是干嘛的

Android 上想做运行时 hook，免不了跟 Xposed 那一套打交道。ImaGate 就是给你提供一个相对干净的入口，让你不用每次从零搭架子：

- **进程注入** —— 目标进程起来之后自动挂上，不需要你手动折腾
- **运行时 hook** —— 基于标准 Xposed API，写起来顺手
- **多版本兼容** —— Android 10~15 日常使用没啥毛病

说白了，它就是一层壳，把那些繁琐的初始化、注入、生命周期管理给包起来了，你专注写自己的逻辑就行。

## 环境要求

- Android 10 及以上
- 已 root，装了 LSPosed 或同类框架
- Kotlin 1.9+

## 怎么用

### 方式一：装 APK

1. 去 [Releases](https://github.com/Superco01-Ti/ImaGate/releases) 下载最新版 APK[1](@context-ref?id=1)
2. 安装后在 LSPosed 里激活模块
3. 勾选要作用的目标应用
4. 重启目标应用，完事

### 方式二：自己构建

```bash
git clone https://github.com/Superco01-Ti/ImaGate.git
cd ImaGate
./gradlew assembleRelease
```

## 目录大概长这样

```
app/src/main/java/com/imagate/
├── GateEntry.kt      # 模块入口
├── ImGate.kt         # 核心逻辑
├── ImaHook.kt        # hook 封装
├── ImatGateway.kt    # 网关调度
└── MainActivity.kt   # 界面
```

## 唠两句

这项目是我自己折腾的产物，代码风格可能有点个人色彩，但该有的注释都有。如果你发现了 bug，或者有更好的思路，欢迎提 issue 或者直接 PR，看到就会回。

你要是想拿它做点什么，随意，记得遵守 GPL-3.0 协议就行。

## License

[GPL-3.0](LICENSE)

> 仅供学习研究使用，请勿用于任何非法用途。
