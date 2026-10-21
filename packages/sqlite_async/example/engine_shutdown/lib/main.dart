import 'dart:async';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:sqlite3/sqlite3.dart' as sqlite;
import 'package:sqlite_async/native.dart';
import 'package:sqlite_async/sqlite_async.dart';

const _channel = MethodChannel('sqlite_diagnostic');
const _sqliteOptions = SqliteOptions(maxReaders: 2);

final class TestFactory extends NativeSqliteOpenFactory {
  TestFactory(String path) : super(path: path, sqliteOptions: _sqliteOptions);

  @override
  sqlite.Database openNativeConnection(SqliteOpenOptions options) {
    final db = sqlite.sqlite3.open(path, mode: options.openMode, mutex: true);
    configureConnection(db, options);
    return db;
  }
}

Future<Map<String, Object>> runCase({required bool graceful}) async {
  debugPrint('probe start graceful=$graceful');
  final directory = await Directory.systemTemp.createTemp('sqlite-shutdown-');
  final path = '${directory.path}/example.sqlite';
  final database = SqliteDatabase.withFactory(TestFactory(path));
  await database.execute('CREATE TABLE example(value INTEGER)');
  await database.execute('INSERT INTO example VALUES (0)');
  final errors = <String, int>{};
  var completed = 0;
  await _channel.invokeMethod<void>('start', path);
  debugPrint('probe worker ready graceful=$graceful');
  final stopped = Completer<void>();
  Timer(const Duration(milliseconds: 100), () async {
    try {
      await _channel.invokeMethod<void>('stop', graceful);
      debugPrint('probe engine stopped graceful=$graceful');
      stopped.complete();
    } catch (error, stack) {
      stopped.completeError(error, stack);
    }
  });

  await Future.wait(
    List.generate(12, (client) async {
      for (var iteration = 0; iteration < 30; iteration++) {
        try {
          if (client == 0) {
            await database.writeTransaction(
              (tx) => tx.execute('UPDATE example SET value = value + 1'),
            );
          } else {
            await database.readTransaction((tx) async {
              final before = await tx.get('SELECT value FROM example');
              await Future<void>.delayed(const Duration(milliseconds: 100));
              final after = await tx.get('SELECT value FROM example');
              if (before['value'] != after['value']) {
                throw StateError('The snapshot changed.');
              }
            });
          }
          completed++;
          if (completed % 60 == 0)
            debugPrint('probe progress graceful=$graceful count=$completed');
        } catch (error) {
          errors.update(
            error.toString(),
            (count) => count + 1,
            ifAbsent: () => 1,
          );
        }
      }
    }),
  );
  debugPrint('probe foreground done graceful=$graceful errors=$errors');
  await stopped.future;
  final result = <String, Object>{
    'shutdown': graceful ? 'drain before destroy' : 'destroy during work',
    'completed': completed,
    'errors': errors,
  };
  await database.close();
  debugPrint('probe closed graceful=$graceful');
  await directory.delete(recursive: true);
  return result;
}

Future<String> runComparison() async {
  final unsafe = await runCase(graceful: false);
  final safe = await runCase(graceful: true);
  final earlyExitPassed = (unsafe['errors']! as Map<String, int>).isEmpty;
  final controlPassed = (safe['errors']! as Map<String, int>).isEmpty;
  return '${earlyExitPassed && controlPassed ? 'PASS' : 'FAIL'}\n$unsafe\n\n$safe';
}

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(
    MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('SQLite engine shutdown')),
        body: FutureBuilder<String>(
          future: runComparison(),
          builder: (context, snapshot) => Text(
            snapshot.hasError
                ? '${snapshot.error}'
                : snapshot.data ?? 'Test active',
          ),
        ),
      ),
    ),
  );
}

@pragma('vm:entry-point')
Future<void> backgroundMain() async {
  WidgetsFlutterBinding.ensureInitialized();
  final path = await _channel.invokeMethod<String>('path');
  final database = SqliteDatabase.withFactory(TestFactory(path!));
  await database.initialize();
  final pending = database.readLock((tx) async {
    await tx.getAll('BEGIN');
    await tx.get('SELECT value FROM example');
    await _channel.invokeMethod<void>('ready');
    await tx.computeWithDatabase<void>((connection) async {
      // This delay makes the engine exit while the worker still owns work.
      sleep(const Duration(milliseconds: 500));
      connection.execute('END TRANSACTION');
    });
  });
  _channel.setMethodCallHandler((call) async {
    if (call.method == 'close') {
      debugPrint('probe worker close request');
      await pending;
      debugPrint('probe worker work finished');
      await database.close();
      debugPrint('probe worker database closed');
      return null;
    }
    throw MissingPluginException(call.method);
  });
  await pending;
  await Completer<void>().future;
}
