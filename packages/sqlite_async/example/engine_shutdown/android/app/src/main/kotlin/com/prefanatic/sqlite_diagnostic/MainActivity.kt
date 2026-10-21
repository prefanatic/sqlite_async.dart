package com.prefanatic.sqlite_diagnostic

import io.flutter.FlutterInjector
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.embedding.engine.dart.DartExecutor
import io.flutter.plugin.common.MethodChannel

class MainActivity : FlutterActivity() {
    private var background: FlutterEngine? = null
    private var workerChannel: MethodChannel? = null

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)
        window.addFlags(android.view.WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        MethodChannel(flutterEngine.dartExecutor.binaryMessenger, "sqlite_diagnostic")
            .setMethodCallHandler { call, result ->
                when (call.method) {
                    "start" -> {
                        val path = call.arguments as String
                        val worker = FlutterEngine(this)
                        background = worker
                        val channel = MethodChannel(worker.dartExecutor.binaryMessenger, "sqlite_diagnostic")
                        workerChannel = channel
                        channel.setMethodCallHandler { request, answer ->
                            when (request.method) {
                                "path" -> answer.success(path)
                                "ready" -> {
                                    answer.success(null)
                                    result.success(null)
                                }
                                else -> answer.notImplemented()
                            }
                        }
                        worker.dartExecutor.executeDartEntrypoint(
                            DartExecutor.DartEntrypoint(
                                FlutterInjector.instance().flutterLoader().findAppBundlePath(),
                                "package:sqlite_diagnostic/main.dart",
                                "backgroundMain"
                            )
                        )
                    }
                    "stop" -> {
                        if (call.arguments == true) {
                            workerChannel!!.invokeMethod("close", null, object : MethodChannel.Result {
                                override fun success(value: Any?) {
                                    background?.destroy()
                                    background = null
                                    result.success(null)
                                }
                                override fun error(code: String, message: String?, details: Any?) {
                                    result.error(code, message, details)
                                }
                                override fun notImplemented() = result.notImplemented()
                            })
                        } else {
                            background?.destroy()
                            background = null
                            result.success(null)
                        }
                    }
                    else -> result.notImplemented()
                }
            }
    }
}
