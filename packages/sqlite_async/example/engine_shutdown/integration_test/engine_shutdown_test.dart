import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlite_diagnostic/main.dart' as app;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('engine destruction preserves foreground transactions', (
    tester,
  ) async {
    final earlyExit = await app.runCase(graceful: false);
    expect(earlyExit['errors'], isEmpty);
    expect(earlyExit['completed'], 360);
    final orderlyExit = await app.runCase(graceful: true);
    expect(orderlyExit['errors'], isEmpty);
    expect(orderlyExit['completed'], 360);
  });
}
