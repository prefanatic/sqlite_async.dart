import 'dart:async';
import 'dart:developer';

import 'package:meta/meta.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite3_connection_pool/sqlite3_connection_pool.dart';
import 'package:sqlite_async/src/common/abstract_open_factory.dart';
import 'package:sqlite_async/src/common/sqlite_database.dart';
import 'package:sqlite_async/src/native/native_sqlite_open_factory.dart';
import 'package:sqlite_async/src/sqlite_connection.dart';

import 'package:sqlite_async/src/update_notification.dart';

import '../../common/mutex.dart';
import '../../common/timeouts.dart';
import '../../impl/context.dart';
import '../../utils/profiler.dart';
import 'worker.dart';
import 'worker_group.dart';

/// A SQLite database instance.
///
/// It is safe to use multiple instances backed by the same database file. In
/// that case, update notifications and connection locks are automatically
/// shared between instances. This also works if the instances are opened on
/// different isolates or Dart/Flutter engines in the same process without prior
/// coordination.
final class NativeSqliteDatabaseImpl extends SqliteDatabaseImpl {
  @override
  final NativeSqliteOpenFactory openFactory;
  late final Future<SqliteConnectionPool> _pool =
      _openNativePool(openFactory).then((pool) {
    // Pipe updates into a stream controller as soon as the pool is opened to
    // avoid missing updates if the stream is only listened to later.
    _updates.addStream(pool.updatedTables.map((e) {
      return UpdateNotification(e.toSet());
    }));

    return pool;
  });
  Future<void>? _closing;
  final _lockGuard = Object();
  int _activeOperations = 0;
  Completer<void>? _operationsDrained;

  @override
  int get maxReaders => openFactory.sqliteOptions.maxReaders;

  @override
  @protected
  Future<void> get isInitialized => _pool;

  late final _readWorkers =
      WorkerGroup(maxReaders > 0 ? maxReaders : 1, _spawnWorker);
  late final _writeWorkers = WorkerGroup(1, _spawnWorker);
  late final _exclusiveWorkers = WorkerGroup(1, _spawnWorker);

  final StreamController<UpdateNotification> _updates =
      StreamController.broadcast(sync: true);

  @override
  Stream<UpdateNotification> get updates => _updates.stream;

  NativeSqliteDatabaseImpl(this.openFactory);

  @override
  bool get closed {
    return _closing != null;
  }

  /// Returns true if the _write_ connection is in auto-commit mode
  /// (no active transaction).
  @override
  Future<bool> getAutoCommit() => _useConnection(
      writer: true,
      debugContext: 'getAutoCommit',
      (context) => context.getAutoCommit());

  @override
  Future<void> close() {
    _checkNotLocked('close');

    return _closing ??= Future.sync(() async {
      _readWorkers.cancelPending();
      _writeWorkers.cancelPending();
      _exclusiveWorkers.cancelPending();
      final pool = await _pool;

      // Close this client's requests without reserving another client's writer.
      // Its read snapshot can need that writer before it releases a reader.
      await [
        pool.close(abortOutstandingRequests: true),
        if (_activeOperations > 0)
          (_operationsDrained ??= Completer<void>()).future,
      ].wait;
      // Active operations return their workers before shutdown closes them.
      await [
        _readWorkers.close(),
        _writeWorkers.close(),
        _exclusiveWorkers.close()
      ].wait;
    });
  }

  @override
  Future<T> abortableReadLock<T>(
      Future<T> Function(SqliteReadContext tx) callback,
      {Future<void>? abortTrigger,
      String? debugContext}) async {
    return _useConnection(
      writer: false,
      debugContext: debugContext ?? 'readLock',
      abortTrigger: abortTrigger,
      (context) => ScopedReadContext.assumeReadLock(context, callback),
    );
  }

  @override
  Future<T> abortableWriteLock<T>(
      Future<T> Function(SqliteWriteContext tx) callback,
      {Future<void>? abortTrigger,
      String? debugContext}) async {
    return _useConnection(
      writer: true,
      debugContext: debugContext ?? 'writeLock',
      abortTrigger: abortTrigger,
      (context) => ScopedWriteContext.assumeWriteLock(context, callback),
    );
  }

  Future<T> _useConnection<T>(
    Future<T> Function(_LeasedContext context) callback, {
    required bool writer,
    required String debugContext,
    Future<void>? abortTrigger,
  }) {
    return _runInLockContext(debugContext, () async {
      await _pool;
      final group = writer ? _writeWorkers : _readWorkers;
      final worker = await group.take(abortTrigger: abortTrigger);
      try {
        if (closed) throw AbortException(debugContext);
        await worker
            .run(_acquire(writer), abortTrigger: abortTrigger)
            .translateAbortExceptions(debugContext);
        final context = _LeasedContext(worker: worker);
        try {
          return await callback(context);
        } finally {
          context.close();
          await worker.run((state, _) => state.release());
        }
      } finally {
        group.giveBack(worker);
      }
    });
  }

  static Future<int> Function(WorkerState, Future<void>) _acquire(bool writer) {
    return (state, abort) => state.acquire(writer: writer, abort: abort);
  }

  @override
  Future<void> refreshSchema() async {
    _checkNotLocked('refreshSchema');
    await withAllConnections((writer, readers) async {
      await Future.wait([
        writer.execute("PRAGMA table_info('sqlite_master')"),
        for (final reader in readers)
          reader.getAll("PRAGMA table_info('sqlite_master')")
      ]);
    });
  }

  @override
  Future<T> withAllConnections<T>(
      Future<T> Function(
              SqliteWriteContext writer, List<SqliteReadContext> readers)
          block) async {
    return _runInLockContext('withAllConnections', () async {
      await _pool;
      final worker = await _exclusiveWorkers.take();
      try {
        if (closed) throw AbortException('withAllConnections');
        final count = await worker
            .run((state, abort) =>
                state.acquire(writer: true, exclusive: true, abort: abort))
            .translateAbortExceptions('withAllConnections');
        final writeExecutor = _LeasedContext(worker: worker);
        final readExecutors = [
          for (var i = 1; i < count; i++)
            _LeasedContext(worker: worker, index: i)
        ];
        final writer = ScopedWriteContext(writeExecutor);
        final readers = [
          for (final reader in readExecutors) ScopedReadContext(reader)
        ];
        try {
          return await block(writer, readers);
        } finally {
          writeExecutor.close();
          for (final reader in readExecutors) {
            reader.close();
          }
          writer.invalidate();
          for (final reader in readers) {
            reader.invalidate();
          }
          await worker.run((state, _) => state.release());
        }
      } finally {
        _exclusiveWorkers.giveBack(worker);
      }
    });
  }

  @override
  Future<ResultSet> execute(String sql,
      [List<Object?> parameters = const []]) async {
    return _useConnection(debugContext: 'execute', writer: true, (ctx) async {
      final rs = await ctx.execute(sql, parameters);
      await ctx.checkNotInTransaction();
      return rs;
    });
  }

  Future<IsolateWorker> _spawnWorker() {
    final factory = openFactory;
    return IsolateWorker.spawn(() => _connectNativePool(factory));
  }

  void _checkNotLocked(String? debugContext) {
    if (Zone.current[_lockGuard] != null) {
      var message =
          'Blocked attempt to use connection object in a read/write lock callback.';
      if (debugContext != null) {
        message +=
            ' Try using `tx.$debugContext` instead of `db.$debugContext`.';
      }

      throw LockError(message);
    }
  }

  Future<T> _runInLockContext<T>(
      String debugContext, Future<T> Function() inner) {
    _checkNotLocked(debugContext);
    if (closed) return Future.error(AbortException(debugContext));
    _activeOperations++;
    return runZoned(() async {
      try {
        return await inner();
      } finally {
        _activeOperations--;
        if (_activeOperations == 0) _operationsDrained?.complete();
      }
    }, zoneValues: {_lockGuard: true});
  }

  static Future<SqliteConnectionPool> _openNativePool(
    NativeSqliteOpenFactory openFactory,
  ) async {
    await openFactory.beforeOpen();

    return SqliteConnectionPool.openAsync(
      name: openFactory.path,
      openConnections: () => _openConnections(openFactory),
    );
  }

  static SqliteConnectionPool _connectNativePool(
      NativeSqliteOpenFactory openFactory) {
    return SqliteConnectionPool.open(
      name: openFactory.path,
      openConnections: () => _openConnections(openFactory),
    );
  }

  static PoolConnections _openConnections(NativeSqliteOpenFactory openFactory) {
    Database openConnection({required bool isWriter}) {
      return openFactory.openNativeConnection(
        SqliteOpenOptions(primaryConnection: isWriter, readOnly: !isWriter),
      );
    }

    return PoolConnections(
      openConnection(isWriter: true),
      [
        for (var i = 0; i < openFactory.sqliteOptions.maxReaders; i++)
          openConnection(isWriter: false)
      ],
      preparedStatementCacheSize:
          openFactory.sqliteOptions.preparedStatementCacheSize,
    );
  }
}

final class _LeasedContext extends UnscopedContext {
  final int index;
  final TimelineTask? task;
  final IsolateWorker worker;

  /// Whether to throw an exception if we're about to execute a statement if
  /// the connection is in autocommit mode.
  final bool verifyInTransaction;

  @override
  bool closed = false;

  _LeasedContext({
    this.index = 0,
    required this.worker,
    this.task,
    this.verifyInTransaction = false,
  });

  @override
  UnscopedContext interceptOutermostTransaction() {
    return _LeasedContext(
      index: index,
      worker: worker,
      task: task,
      verifyInTransaction: true,
    );
  }

  void close() {
    closed = true;
  }

  Future<T> _runOnWorker<T>(
      FutureOr<T> Function(PoolConnection db) compute) async {
    return worker.run(_wrapDbClosure(index, verifyInTransaction, compute));
  }

  @override
  Future<T> computeWithDatabase<T>(
      FutureOr<T> Function(CommonDatabase db) compute) {
    return _runOnWorker((db) => compute(db.database));
  }

  @override
  Future<ResultSet> execute(String sql, List<Object?> parameters) {
    return task.timeAsync('execute', sql: sql, parameters: parameters, () {
      return _runOnWorker(_select(sql, parameters));
    });
  }

  @override
  Future<void> executeBatch(String sql, List<dynamic> parameterSets) {
    // TODO: Make parameterSets a List<List<Object?>>
    return task.timeAsync('executeMultiple', sql: sql, () {
      return _runOnWorker(_executeBatch(sql, parameterSets));
    });
  }

  @override
  Future<void> executeMultiple(String sql) {
    return task.timeAsync('executeMultiple', sql: sql, () {
      return _runOnWorker(_selectMultiple(sql));
    });
  }

  @override
  Future<ResultSet> getAll(String sql, [List<Object?> parameters = const []]) {
    return execute(sql, parameters);
  }

  @override
  Future<bool> getAutoCommit() {
    return worker
        .run(_wrapDbClosure(index, false, (db) => db.database.autocommit));
  }

  static Future<T> Function(WorkerState, Future<void>) _wrapDbClosure<T>(
      int index,
      bool checkInTransaction,
      FutureOr<T> Function(PoolConnection) compute) {
    return (state, _) => state.compute(index, checkInTransaction, compute);
  }

  static ResultSet Function(PoolConnection) _select(
      String sql, List<Object?> parameters) {
    return (db) => db.select(sql, parameters);
  }

  static void Function(PoolConnection) _executeBatch(
      String sql, List<dynamic> parameterSets) {
    return (conn) {
      final stmt = conn.database.prepare(sql, checkNoTail: true);
      try {
        for (final instantiation in parameterSets) {
          stmt.execute(instantiation);
        }
      } finally {
        stmt.close();
      }
    };
  }

  static void Function(PoolConnection) _selectMultiple(String sql) {
    return (db) => db.execute(sql);
  }
}

extension<T> on Future<T> {
  Future<T> translateAbortExceptions(String debugContext) {
    return onError<PoolAbortException>(
        (e, s) => Error.throwWithStackTrace(AbortException(debugContext), s));
  }
}
