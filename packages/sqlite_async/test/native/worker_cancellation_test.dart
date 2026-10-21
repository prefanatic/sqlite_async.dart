@TestOn('!browser')
library;

import 'dart:async';

import 'package:sqlite_async/sqlite_async.dart';
import 'package:test/test.dart';

import '../utils/test_utils_impl.dart';

void main() {
  final utils = TestUtils();
  for (final writer in [false, true]) {
    test(
        'close cancels a worker that waits for another client\'s ${writer ? 'writer' : 'reader'}',
        () async {
      final path = utils.dbPath();
      await utils.cleanDb(path: path);
      const options = SqliteOptions(maxReaders: 1);
      final owner = SqliteDatabase(path: path, options: options);
      final waiting = SqliteDatabase(path: path, options: options);
      final entered = Completer<void>();
      final release = Completer<void>();
      addTearDown(() async {
        if (!release.isCompleted) release.complete();
        await waiting.close();
        await owner.close();
        await utils.cleanDb(path: path);
      });
      Future<void> hold(SqliteReadContext tx) async {
        await tx.get('SELECT 1');
        entered.complete();
        await release.future;
      }

      final holding = writer ? owner.writeLock(hold) : owner.readLock(hold);
      await entered.future;
      final operation = writer
          ? waiting.writeLock((tx) async {})
          : waiting.readLock((tx) async {});
      final aborted = expectLater(operation, throwsA(isA<AbortException>()));
      await Future<void>.delayed(const Duration(milliseconds: 30));
      await waiting.close().timeout(const Duration(seconds: 2));
      await aborted;
      expect(release.isCompleted, isFalse);
      release.complete();
      await holding;
      expect(await owner.get('SELECT 1 AS value'), {'value': 1});
    });
  }
}
