# Worker lease after owner exit

Run this example from the sqlite_async package directory:

```
dart run example/owner_exit/bin/owner_exit.dart reader
dart run example/owner_exit/bin/owner_exit.dart writer
dart run example/owner_exit/bin/owner_exit.dart exclusive
```

The example uses synthetic data and one reader.
A separate isolate starts a transaction through its SQLite worker.
The main isolate kills that owner while the worker executes a delayed operation.
The worker must finish its operation before the pool returns the lease.
The main isolate then verifies a new transaction and database integrity.
Each run has a five-second deadline.

The old revision 4724a35e129555062f1d5aa87735a8cbc19db56c times out in the reader test.
The patched implementation passes all three modes.
The native test suite runs these examples in separate processes.
