import 'dart:async';
import 'dart:isolate';

import 'package:async/async.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite3_connection_pool/sqlite3_connection_pool.dart';

const _ownerExited = 'sqlite_async: owner exited';
const _abortPending = 'sqlite_async: abort pending';

/// A worker that owns its pool client and connection leases.
///
/// Native finalizers cannot return a lease while this isolate executes SQLite.
final class IsolateWorker {
  final ReceivePort _receiveResponses;
  final SendPort _sendCommands;
  final Map<int, Completer<Object?>> _outstandingWorkItems = {};
  int _nextWorkItem = 0;
  var _closeRequested = false;
  late final Future<void> _closeAcknowledged;

  IsolateWorker._(this._sendCommands, this._receiveResponses) {
    _closeAcknowledged = _receiveResponses.listen((Object? message) {
      if (message == null) {
        _closeRequested = true;
        for (final pending in _outstandingWorkItems.values) {
          pending.completeError(StateError('Worker closed'));
        }
        _outstandingWorkItems.clear();
        _receiveResponses.close();
        return;
      }

      final WorkResult(:id, :result) = message as WorkResult;
      if (_outstandingWorkItems.remove(id) case final completer?) {
        switch (result) {
          case ValueResult(:final value):
            completer.complete(value);
          case ErrorResult(:final error, :final stackTrace):
            completer.completeError(error, stackTrace);
        }
      }
    }).asFuture();
  }

  Future<T> run<T>(
      FutureOr<T> Function(WorkerState state, Future<void> abort) task,
      {Future<void>? abortTrigger}) async {
    if (_closeRequested) throw StateError('Isolate worker closed');
    final id = _nextWorkItem++;
    final completer = _outstandingWorkItems[id] = Completer();
    try {
      _sendCommands.send(WorkItem(id, _receiveResponses.sendPort, task));
    } catch (_) {
      _outstandingWorkItems.remove(id);
      rethrow;
    }
    abortTrigger?.whenComplete(() {
      if (_outstandingWorkItems.containsKey(id)) {
        _sendCommands.send(AbortWork(id));
      }
    });
    return (await completer.future) as T;
  }

  void abortPending() => _sendCommands.send(_abortPending);

  Future<void> close() async {
    if (_closeRequested) return _closeAcknowledged;
    _closeRequested = true;
    // The caller has drained all operations before it closes an idle worker.
    _sendCommands.send(_ownerExited);
    await _closeAcknowledged;
    Isolate.current.removeOnExitListener(_sendCommands);
  }

  static Future<IsolateWorker> spawn(
      SqliteConnectionPool Function() openPool) async {
    final receiveSendPort = ReceivePort();
    final receiveResponses = ReceivePort('isolate worker');
    await Isolate.spawn(
      _entrypoint,
      (receiveSendPort.sendPort, openPool),
      onExit: receiveResponses.sendPort,
    );
    final response = await receiveSendPort.first;
    if (response case ErrorResult(:final error, :final stackTrace)) {
      receiveResponses.close();
      Error.throwWithStackTrace(error, stackTrace);
    }
    final port = response as SendPort;
    Isolate.current.addOnExitListener(port, response: _ownerExited);
    return IsolateWorker._(port, receiveResponses);
  }

  static Future<void> _entrypoint(
      (SendPort, SqliteConnectionPool Function()) arguments) async {
    final (sendPort, openPool) = arguments;
    final receiveTasks = ReceivePort('receive tasks');
    final WorkerState state;
    try {
      state = WorkerState(openPool());
    } catch (error, stack) {
      receiveTasks.close();
      sendPort.send(ErrorResult(error, stack));
      return;
    }
    sendPort.send(receiveTasks.sendPort);
    final aborts = <int, Completer<void>>{};
    var pending = Future<void>.value();
    receiveTasks.listen((Object? message) {
      switch (message) {
        case WorkItem item:
          final abort = aborts[item.id] = Completer<void>();
          pending = pending.then((_) async {
            await item.handle(state, abort.future);
            aborts.remove(item.id);
          });
        case AbortWork(:final id):
          final abort = aborts[id];
          if (abort != null && !abort.isCompleted) abort.complete();
        case _abortPending:
          for (final abort in aborts.values) {
            if (!abort.isCompleted) abort.complete();
          }
        case _ownerExited:
          // This event cannot run during a synchronous SQLite operation.
          // Stop paused Dart callbacks before they can use a returned lease.
          state.close();
          Isolate.exit();
      }
    });
  }
}

/// Resources stay in the isolate that executes each database operation.
final class WorkerState {
  final SqliteConnectionPool pool;
  ConnectionLease? _lease;
  ExclusivePoolAccess? _exclusive;
  List<AsyncConnection> _connections = const [];

  WorkerState(this.pool);

  Future<int> acquire(
      {required bool writer,
      bool exclusive = false,
      required Future<void> abort}) async {
    if (_connections.isNotEmpty) throw StateError('Worker already has a lease');
    if (exclusive) {
      final access =
          _exclusive = await pool.exclusiveAccess(abortSignal: abort);
      _connections = [access.writer, ...access.readers];
    } else {
      final lease = _lease = await (writer
          ? pool.writer(abortSignal: abort)
          : pool.reader(abortSignal: abort));
      _connections = [lease];
    }
    return _connections.length;
  }

  void close() {
    release();
    unawaited(pool.close(abortOutstandingRequests: true));
  }

  void release() {
    _connections = const [];
    final lease = _lease;
    _lease = null;
    final exclusive = _exclusive;
    _exclusive = null;
    lease?.returnLease();
    exclusive?.close();
  }

  Future<T> compute<T>(int index, bool verifyInTransaction,
      FutureOr<T> Function(PoolConnection) compute) async {
    final connection = _connections[index];
    final result = await connection.unsafeAccess((db) async {
      if (verifyInTransaction && db.database.autocommit) {
        throw SqliteException(
          extendedResultCode: 0,
          message: 'Transaction rolled back by earlier statement',
        );
      }
      return await compute(db);
    });
    if (connection case final ConnectionLease lease) {
      await lease.notifyUpdates();
    }
    return result;
  }
}

final class WorkItem {
  final int id;
  final SendPort completePort;
  final FutureOr<Object?> Function(WorkerState, Future<void>) task;

  WorkItem(this.id, this.completePort, this.task);

  Future<void> handle(WorkerState state, Future<void> abort) async {
    final result = await runZonedGuarded(
      () => Result.capture(Future.sync(() => task(state, abort))),
      (Object error, StackTrace stack) {
        completePort.send(WorkResult(id, ErrorResult(error, stack)));
      },
    );
    if (result != null) completePort.send(WorkResult(id, result));
  }
}

final class WorkResult {
  final int id;
  final Result<Object?> result;

  WorkResult(this.id, this.result);
}

final class AbortWork {
  final int id;
  const AbortWork(this.id);
}
