import 'dart:async';
import 'dart:collection';

import '../../common/timeouts.dart';
import 'worker.dart';

/// Bounds worker creation without reserving another connection type's workers.
final class WorkerGroup {
  final int capacity;
  final Future<IsolateWorker> Function() spawn;
  final _idle = Queue<IsolateWorker>();
  final _waiting = Queue<Completer<IsolateWorker>>();
  final _workers = <IsolateWorker>[];
  int _starting = 0;
  bool _closed = false;

  WorkerGroup(this.capacity, this.spawn);

  Future<IsolateWorker> take({Future<void>? abortTrigger}) async {
    if (_closed) throw AbortException('worker');
    if (_idle.isNotEmpty) return _idle.removeFirst();
    if (_workers.length + _starting < capacity) {
      _starting++;
      try {
        final worker = await spawn();
        _workers.add(worker);
        if (_closed) throw AbortException('worker');
        return worker;
      } catch (error, stack) {
        // No waiter may depend on a worker that failed to start.
        while (_waiting.isNotEmpty) {
          _waiting.removeFirst().completeError(error, stack);
        }
        rethrow;
      } finally {
        _starting--;
      }
    }

    final waiter = Completer<IsolateWorker>();
    _waiting.addLast(waiter);
    abortTrigger?.whenComplete(() {
      if (_waiting.remove(waiter)) {
        waiter.completeError(AbortException('worker'));
      }
    });
    return waiter.future;
  }

  void giveBack(IsolateWorker worker) {
    if (_waiting.isNotEmpty) {
      _waiting.removeFirst().complete(worker);
    } else {
      _idle.addLast(worker);
    }
  }

  void cancelPending() {
    _closed = true;
    for (final worker in _workers) {
      worker.abortPending();
    }
    while (_waiting.isNotEmpty) {
      _waiting.removeFirst().completeError(AbortException('worker'));
    }
  }

  /// The owner must drain its active operations before it closes the group.
  Future<void> close() => _workers.map((worker) => worker.close()).wait;
}
