@TestOn('!browser')
library;

import 'dart:io';

import 'package:test/test.dart';

void main() {
  for (final mode in ['reader', 'writer', 'exclusive']) {
    test('worker retains the $mode lease after its owner exits', () async {
      final process = await Process.start(Platform.resolvedExecutable,
          ['run', 'example/owner_exit/bin/owner_exit.dart', mode]);
      final output = process.stdout.transform(systemEncoding.decoder).join();
      final errors = process.stderr.transform(systemEncoding.decoder).join();
      final code = await process.exitCode.timeout(const Duration(seconds: 20),
          onTimeout: () {
        process.kill();
        throw StateError('The owner-exit example did not finish.');
      });
      expect(code, 0, reason: await errors);
      expect(await output, contains('$mode: lease released after owner exit'));
    });
  }
}
