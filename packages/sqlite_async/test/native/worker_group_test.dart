@TestOn('!browser')
library;

import 'dart:async';

import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite3_connection_pool/sqlite3_connection_pool.dart';
import 'package:sqlite_async/sqlite_async.dart';
import 'package:sqlite_async/src/native/database/worker.dart';
import 'package:sqlite_async/src/native/database/worker_group.dart';
import 'package:test/test.dart';

Future<IsolateWorker> _spawnWorker() =>
    IsolateWorker.spawn(() => SqliteConnectionPool.open(
          name: 'worker-group-test',
          openConnections: () => PoolConnections(sqlite3.openInMemory(), []),
        ));

void main() {
  test('queued operations reuse a bounded worker', () async {
    var starts = 0;
    final group = WorkerGroup(1, () {
      starts++;
      return _spawnWorker();
    });
    addTearDown(group.close);
    final first = await group.take();
    var entered = false;
    final second = group.take().then((worker) {
      entered = true;
      return worker;
    });
    await Future<void>.delayed(Duration.zero);
    expect(entered, isFalse);
    expect(starts, 1);
    group.giveBack(first);
    final reused = await second;
    expect(identical(reused, first), isTrue);
    group.giveBack(reused);
  });

  test('a queued operation can abort without consuming a worker', () async {
    final group = WorkerGroup(1, _spawnWorker);
    addTearDown(group.close);
    final first = await group.take();
    final abort = Completer<void>();
    final second = group.take(abortTrigger: abort.future);
    final rejected = expectLater(second, throwsA(isA<AbortException>()));
    abort.complete();
    await rejected;
    group.giveBack(first);
    final reused = await group.take();
    expect(identical(reused, first), isTrue);
    group.giveBack(reused);
  });

  test('worker startup failure rejects queued operations', () async {
    final start = Completer<IsolateWorker>();
    final group = WorkerGroup(1, () => start.future);
    addTearDown(group.close);
    final first = expectLater(group.take(), throwsStateError);
    final second = expectLater(group.take(), throwsStateError);
    start.completeError(StateError('The worker did not start.'));
    await Future.wait([first, second]);
  });

  test('worker reports a pool startup failure', () async {
    await expectLater(
      IsolateWorker.spawn(() => throw StateError('The pool did not open.')),
      throwsStateError,
    );
  });
}
