# SQLite work during Flutter engine shutdown

This Android example uses synthetic data and two Flutter engines.
It uses public sqlite_async APIs without a library patch.

Run `flutter pub get` in this directory.
Run `flutter run --release -d <android-device>`.
Keep the app open until it shows PASS or FAIL.

The example performs two tests:

1. The background engine holds a reader and sends work to a SQLite worker.
   The worker waits 500 milliseconds before it executes END TRANSACTION.
   The main engine destroys the background engine after 100 milliseconds.
   The main engine also performs concurrent transactions through the same native pool.
2. The same test waits for background work and database closure before it destroys the background engine.

The explicit BEGIN and END statements run inside readLock.
They reproduce the transaction boundaries that readTransaction normally adds.
The delay controls the overlap without a library patch.
The example never uses a transaction object after its callback completes.

PASS requires all 360 operations to complete without errors in both tests.
The old pin, 4724a35e129555062f1d5aa87735a8cbc19db56c, fails the early-exit test.
It returns cannot commit - no transaction is active.
The patched fork completes both tests without an error.

Run the automated test with this command:

```
flutter drive --driver=test_driver/integration_test.dart --target=integration_test/engine_shutdown_test.dart --profile -d <android-device>
```

The old implementation keeps the native lease finalizer in the calling engine.
It sends only a connection pointer to the SQLite worker.
Engine destruction can return that lease before the worker finishes.
The patched implementation acquires and uses the lease in the same worker.
Each worker also owns its pool client.
The calling engine does not receive a native connection pointer.

The example uses the local sqlite_async package.
The other dependencies are sqlite3 3.5.2 and sqlite3_connection_pool 0.2.12.
The verified runtime was Flutter 3.47.5 with Dart 3.13.4 on a Pixel 10 Pro.
