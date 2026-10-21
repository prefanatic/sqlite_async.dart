import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'package:sqlite_async/sqlite_async.dart';

Future<void> main(List<String> arguments) async {
  final mode = arguments.single;
  final directory = await Directory.systemTemp.createTemp('sqlite-owner-exit-');
  final path = '${directory.path}/example.sqlite';
  final database = SqliteDatabase(
      path: path,
      options: const SqliteOptions(
        maxReaders: 1,
        preparedStatementCacheSize: 64,
      ));
  final events = ReceivePort();
  final started = Completer<void>();
  final finished = Completer<void>();
  events.listen((message) {
    if (message == 'started') started.complete();
    if (message == 'finished') finished.complete();
  });
  try {
    await database.execute('CREATE TABLE example(value INTEGER)');
    await database.execute('INSERT INTO example VALUES (0)');
    final owner = await Isolate.spawn(_owner, (path, mode, events.sendPort));
    await started.future.timeout(const Duration(seconds: 5));
    owner.kill(priority: Isolate.immediate);
    if (mode == 'reader') {
      await database.readTransaction((tx) async {
        final row = await tx.get('SELECT value FROM example');
        await Future<void>.delayed(const Duration(milliseconds: 30));
        final again = await tx.get('SELECT value FROM example');
        if (row['value'] != again['value']) {
          throw StateError('Snapshot changed');
        }
      }).timeout(const Duration(seconds: 5));
    } else {
      await database
          .writeTransaction((tx) => tx.execute(
                'UPDATE example SET value = value + 1',
              ))
          .timeout(const Duration(seconds: 5));
    }
    await finished.future.timeout(const Duration(seconds: 5));
    final integrity = await database.get('PRAGMA integrity_check');
    if (integrity.values.single != 'ok') throw StateError('$integrity');
    await database.close();
    await directory.delete(recursive: true);
    stdout.writeln('$mode: lease released after owner exit; integrity ok');
    exit(0);
  } catch (error, stack) {
    stderr.writeln('$mode: $error\n$stack');
    exit(1);
  }
}

Future<void> _owner((String, String, SendPort) arguments) async {
  final (path, mode, events) = arguments;
  final database = SqliteDatabase(
      path: path,
      options: const SqliteOptions(
        maxReaders: 1,
        preparedStatementCacheSize: 64,
      ));
  Future<void> use(SqliteReadContext tx) async {
    await tx.getAll('BEGIN');
    await tx.computeWithDatabase<void>((db) async {
      events.send('started');
      sleep(const Duration(milliseconds: 300));
      db.execute('END TRANSACTION');
      events.send('finished');
    });
  }

  switch (mode) {
    case 'reader':
      await database.readLock(use);
    case 'writer':
      await database.writeLock(use);
    case 'exclusive':
      await database.withAllConnections((writer, readers) => use(writer));
    default:
      throw ArgumentError.value(mode, 'mode');
  }
  await Completer<void>().future;
}
