"""Installed debug acceptance app only. Runs actual separate Android processes."""
import argparse, json, subprocess, time
p = argparse.ArgumentParser(); p.add_argument('--serial', required=True); p.add_argument('--app', required=True); args = p.parse_args()
def adb(*values):
    return subprocess.check_output(['adb', '-s', args.serial, *values], text=True, timeout=15)
def launch(phase):
    adb('shell', 'am', 'force-stop', args.app)
    adb('shell', 'run-as', args.app, 'rm', '-f', 'files/ota-observed.json')
    adb('shell', 'am', 'start', '-W', '-n', args.app + '/org.neutron.ota.NeutronOTAProcessFixture', '--es', 'ota_phase', phase)
    for _ in range(20):
        try: return json.loads(adb('shell', 'run-as', args.app, 'cat', 'files/ota-observed.json'))
        except (subprocess.CalledProcessError, json.JSONDecodeError): time.sleep(.1)
    raise AssertionError('Native boot telemetry unavailable')
# Requires a new acceptance-app data directory; no automatic destructive reset.
s = launch('stage'); assert s['pendingUpdateId'] == 'signed-fixture-1'
for expected in range(3):
    s = launch('boot'); assert s['currentUpdateId'] == 'signed-fixture-1'; assert s['consecutiveCrashes'] == expected
s = launch('boot'); assert s['currentUpdateId'] is None and s['buildNumber'] == 1 and s['consecutiveCrashes'] == 0
s = launch('healthy'); assert not s['launchPending'] and s['consecutiveCrashes'] == 0
print('PASS: native Android killed-process rollback and healthy confirmation')
