#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded owned IBM i boundary, native prefix, live normal-task handoff and restart."""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import traceback
import zipfile


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


handoff = module('handoff', 'dcg-online-handoff-probe.py')
prospective = module('prospective', 'dcg-prospective-probe.py')
native = module('native_launcher', 'dcg-native-connect-probe.py')
base = handoff.base
CLASS = 'io.debezium.connector.db2as400.As400LiveHandoffProbeConnector'
CAPTURED_CLASS = handoff.CLASS


def build_bundle(exp, destination):
    """Transport fresh owned observations; Java remains the authoritative validator."""
    def read(name):
        return json.loads((exp.directory / name).read_text())

    def capture(name):
        result = read(name + '/capture.json')
        for entry in result['entries']:
            path = exp.directory / name / entry['file']
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != entry['sha256']:
                raise ValueError('Native bytes changed before handoff')
            entry['raw'] = base64.b64encode(raw).decode()
        return result

    if exp.report['status'] != 'captured-unqualified' or not exp.fixture.created:
        raise ValueError('Fresh owned source observations required')
    boundary = {'profile': 'native-short-boundary-observation-v1', 'startup_proof': False}
    for name in ['before', 'after']:
        boundary[name] = read('candidate/' + name + '/capture.json')
    for name in ['request', 'interval']:
        boundary[name] = read('candidate/' + name + '.json')
    boundary['pulse'] = read('candidate/pulse/pulse.json')
    boundary['statement'] = (exp.directory / 'candidate/pulse/statement.sql').read_text().rstrip()
    boundary['raw'] = capture('candidate/raw')
    result = {'profile': 'prospective-native-boundary-v1', 'startup_proof': False, 'metadata': boundary['before'],
              'boundary': boundary, 'origin': exp.report['origin'], 'cut': exp.report['cut'],
              'older-controls': capture('older-controls'), 'prospective-rows': capture('prospective-rows'),
              'history_discovery': read('candidate/pending/capture.json')}
    destination.write_text(json.dumps(result, indent=2))
    replay = destination.with_suffix('.replay.json')
    with (exp.directory / 'live-input-validation.log').open('w') as log:
        subprocess.run(exp.java_command('As400LiveHandoffProbeConnector', 'verify', destination, replay), check=True, timeout=60,
                       stdout=log, stderr=subprocess.STDOUT)
    return destination, replay


def terminal_position(rows, job, entry_type):
    """The output checkpoint follows the transaction terminal, not later metadata."""
    number, user, name = job.split('/')
    # DISPLAY_JOURNAL exposes fixed-width CHAR fields; preserve leading and
    # internal characters and remove only the SQL right padding.
    def text(row, key):
        value = row[key]
        if not isinstance(value, str):
            raise ValueError('Native journal character field required')
        return value.rstrip(' ')
    matches = [r for r in rows if text(r, 'JOURNAL_CODE') == 'C' and text(r, 'JOURNAL_ENTRY_TYPE') == entry_type
               and tuple(text(r, key) for key in ['JOB_NUMBER', 'JOB_USER', 'JOB_NAME']) == (number, user, name)]
    if len(matches) != 1 or int(matches[0]['COMMIT_CYCLE']) <= 0:
        raise ValueError('Exactly one native transaction terminal for the writer required')
    return int(matches[0]['SEQUENCE_NUMBER'])


def drain_capture_sessions(capture):
    """Close completed boundary sessions before a long Kafka run, retaining errors."""
    errors = []
    for connection in list(reversed(capture.connections)):
        try:
            if not connection.autocommit:
                connection.rollback()
        except Exception as error:
            errors.append(capture.fixture.redact(str(error)))
        finally:
            try:
                connection.close()
                capture.connections.remove(connection)
            except Exception as error:
                errors.append(capture.fixture.redact(str(error)))
    capture.report['boundary_session_drain'] = {'status': 'failed' if errors else 'passed', 'errors': errors}
    capture.persist()
    if errors:
        raise RuntimeError('Boundary sessions did not drain; see retained report')


def cleanup_owned(experiment, capture):
    """Always drain fixture sessions; require worker cleanup before deleting objects."""
    failure = None
    try:
        if experiment is not None:
            experiment.cleanup()
            if (experiment.report.get('cleanup', {}).get('status') != 'passed'
                    or experiment.report.get('native_worker_cleanup', {}).get('status') != 'passed'):
                raise RuntimeError('Native worker cleanup was not verified')
    except BaseException as error:
        failure = error
    if failure is None:
        capture.cleanup()
        return
    # A Docker/worker failure must not skip our owned writer rollback/close.
    # It also cannot authorize deletion while a source worker could still run.
    try:
        experiment.report['status'] = 'failed'
        experiment.report['cleanup_failure'] = capture.fixture.redact(str(failure))
        try:
            experiment.save()
        finally:
            drain_capture_sessions(capture)
    finally:
        capture.report['fixture_cleanup'] = {'status': 'blocked',
                'error': 'Worker cleanup incomplete; fixture retained for verified cleanup',
                'worker_error': capture.fixture.redact(str(failure))}
        capture.persist()
    raise failure


def export_live(source, replay_file, cut):
    bundle, verified = json.loads(source.read_text()), json.loads(replay_file.read_text())
    if (bundle['profile'] != 'prospective-native-boundary-v1' or bundle['startup_proof'] is not False
            or verified['status'] != 'verified-offline' or int(bundle['cut']['sequence']) != cut
            or any(c['status'] != 'passed' for c in verified['recovery']['cases'])):
        raise ValueError('Strict native candidate replay required')
    entries = bundle['prospective-rows']['entries']
    # Independent controlled SQL ledger: initial A10 is the acknowledged legacy
    # seed; M21/A12 are its remaining prefix, then A14 and committed B clear/101.
    rows = []
    for entry in entries:
        if entry['type'] == 'R/PT':
            raw = base64.b64decode(entry['raw'])
            offset = struct.unpack_from('>i', raw, 8)[0] + 16
            rows.append({'file': entry['table'], 'op': 'c', 'sequence': entry['position']['sequence'],
                         'id': struct.unpack_from('>i', raw, offset)[0]})
    if [(r['file'], r['id']) for r in rows] != [('A', 10), ('M', 21), ('A', 12), ('B', 101), ('A', 14)]:
        raise ValueError('Native fixture differs from its independent writer ledger')
    clears = [e for e in entries if e['type'] == 'D/CG']
    if len(clears) != 1 or clears[0]['table'] != 'B':
        raise ValueError('Exactly one captured B clear required')
    prefix = rows[1:3]
    clear = {'file': 'B', 'op': 't', 'sequence': clears[0]['position']['sequence']}
    bundle.update(library=bundle['prospective-rows']['library'], entries=entries,
                  native_identity_evidence=bundle['prospective-rows']['native_identity_evidence'])
    return bundle, {'prefix': prefix, 'all': prefix + [rows[4], clear, rows[3]], 'candidate_W': str(cut),
                    'fault_truncate_sequence': clear['sequence'], 'prospective': True}


class LiveExperiment(handoff.HandoffExperiment):
    def __init__(self, directory, capture, bundle, replay):
        handoff.export_prospective = export_live
        os.environ['DCG_HANDOFF_STANDARD_HISTORY'] = 'true'
        os.environ['DCG_HANDOFF_NORMAL_RECOVERY'] = 'true'
        super().__init__(directory, bundle, replay, int(capture.report['cut']['sequence']))
        self.capture = capture
        self.fixture, self.library = capture.fixture, capture.library
        self.egress = self.prefix + '-native'
        self.children, self.log_threads, self.logs = [], [], {}
        addresses = {r[4][0] for r in socket.getaddrinfo(self.fixture.target.host, None, socket.AF_INET, socket.SOCK_STREAM)}
        if len(addresses) != 1:
            raise ValueError('Exact native test address required')
        self.native_host = self.fixture.target.host + ':' + addresses.pop()

    java = native.NativeExperiment.java

    def stage(self):
        super().stage()
        classes = handoff.ROOT / 'debezium-connector-ibmi/target/test-classes'
        with zipfile.ZipFile(self.plugin / 'synthetic-probe.jar', 'a') as jar:
            for name in ['As400LiveHandoffProbeConnector', 'As400ContinuousConnectSource', 'As400NativeInitializationProbe']:
                for path in classes.rglob(name + '*.class'):
                    jar.write(path, str(path.relative_to(classes)))
        (self.plugin / 'native-console.properties').write_text('\n'.join([
            'status=warn', 'name=NativeHandoffConsole', 'appender.console.type=Console', 'appender.console.name=Console',
            'appender.console.layout.type=PatternLayout', 'appender.console.layout.pattern=%d{ISO8601} %-5p %c - %m%n',
            'rootLogger.level=info', 'rootLogger.appenderRef.console.ref=Console', '']))
        self.report.update(source_connected=True, source_transport='Actual JT400 legacy-prefix re-read and continuous native reader',
                           scope='Controller-owned native boundary; normal task legacy handoff, Kafka acknowledgement, live continuation and fresh-task recovery',
                           origin='Fresh isolated native boundary; fixed-schema/retention obligations remain; no general startup proof')
        self.report['plugin_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        files = [Path(__file__), Path(__file__).with_name('dcg-native-fixture.py'), Path(__file__).with_name('dcg-prospective-probe.py')]
        files += [handoff.ROOT / 'debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400' / (name + '.java')
                  for name in ['As400LiveHandoffProbeConnector', 'As400ContinuousConnectSource', 'As400HandoffBindingHistory']]
        self.report['harness_sha256'].update({str(p.relative_to(handoff.ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
        self.save()

    def start_broker(self):
        super().start_broker()
        self.command('docker', 'network', 'create', '--label', 'dcg.local.probe=' + self.prefix, self.egress)

    def container(self, name, args):
        config = self.directory / (name + '.properties')
        if config.exists():
            with config.open('a') as stream:
                stream.write('request.timeout.ms=5000\ndefault.api.timeout.ms=10000\nworker.sync.timeout.ms=20000\nrebalance.timeout.ms=30000\n')
            worker = base.Experiment.container(self, name, ['--user', f'{os.getuid()}:{os.getgid()}', '--add-host', self.native_host,
                         '-v', f'{self.plugin}:/probe:ro', '-v', f'{self.control}:/control', '-v', f'{config}:/worker.properties:ro',
                         '-e', 'PROBE_WORKER=' + name, '--entrypoint', '/bin/sh', base.IMAGE, '-c', 'sleep 2400'])
            self.command('docker', 'network', 'connect', '--gw-priority', '1', self.egress, worker)
            _, path = self.java(worker, [CLASS, 'worker', '/worker.properties'], name)
            self.logs[worker] = path
            return worker
        return base.Experiment.container(self, name, args)

    def command(self, *args, **kwargs):
        if len(args) >= 3 and args[:2] == ('docker', 'logs') and args[-1] in getattr(self, 'logs', {}):
            path = self.logs[args[-1]]
            return path.read_text() if path.exists() else ''
        return super().command(*args, **kwargs)

    def rest(self, worker, endpoint, payload=None, tolerate=False):
        if payload and isinstance(payload, dict) and payload.get('config', {}).get('connector.class') == CAPTURED_CLASS:
            config = payload['config']
            config.update({'connector.class': CLASS, 'database.hostname': 'stdin-native-handoff.invalid',
                           'database.dbname': self.fixture.server_name, 'probe.native.history.discovery': 'true'})
            config.pop('producer.override.interceptor.classes', None)
            (self.control / (payload['name'] + '.owned-fixture')).write_text(self.library)
            (self.directory / 'live-connector.json').write_text(json.dumps(config, indent=2))
        return super().rest(worker, endpoint, payload, tolerate=tolerate)

    def progressed(self, sequence, label):
        return base.wait_for(label, lambda: value if (value := self.checkpoint(self.observer_container, 'handofflive', label))
                            and int(value.get('completed_through.sequence', 0)) >= int(sequence) else None, 150)

    def terminal(self, observer, start, job, entry_type, label):
        end = prospective.boundary.point(observer[0], self.library)
        params = [self.library, 'QSQJRN', start['library'], start['receiver'], start['sequence'],
                  end['library'], end['receiver'], end['sequence'], start['sequence']]
        rows = prospective.census.rows(observer[0], prospective.boundary.AUDIT_SQL, params, 2000)
        path = self.directory / (label + '-native-terminal.json')
        observation = {'sql': prospective.boundary.AUDIT_SQL, 'parameters': params, 'rows': rows,
                       'writer': job, 'observed_tail': end}
        prospective.census.save(path, observation)
        position = terminal_position(rows, job, entry_type)
        prospective.census.save(path, {**observation, 'terminal': position})
        return position

    def exercise_native(self):
        scenario = 'handofflive'
        first = self.start_worker(scenario, 'w1')
        _, generation = self.create_handoff(first, scenario)
        baseline, _ = self.complete(scenario, generation)
        self.check('Actual native legacy prefix and live reader used', bool(list(self.control.glob('live-reader-*/live-legacy-prefix.json')))
                   and (self.control / (generation + '.live-reader.json')).exists(), 'control/')
        exp = self.capture
        exp.deadline = None
        writer, observer, transaction = exp.connection(), exp.connection(read_only=True), exp.connection(transaction=True)
        exp.execute(transaction, 'live-clear-open', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(transaction, 'live-clear-reload', f'INSERT INTO {self.library}.B VALUES (200)')
        old_receiver, new_receiver = exp.fixture.rotate()
        exp.execute(writer, 'live-unrelated-progress', f'INSERT INTO {self.library}.A VALUES (200)')
        position = prospective.boundary.point(observer[0], self.library)
        saved = self.progressed(position['sequence'], 'unrelated-open')
        before_restart = self.inspect(scenario, 'unrelated-open')
        self.check('Unrelated table advances beyond captured range while real clear remains open',
                   int(saved['completed_through.sequence']) > int(self.bundle['entries'][-1]['position']['sequence'])
                   and saved['replay_from.inclusive'] == 'true'
                   and [(r['value']['op'], (r['value'].get('after') or {}).get('ID')) for r in before_restart[len(baseline):]] == [('c', 200)], 'control/')
        self.check('Open clear retains F in old receiver while durable W crosses native rotation',
                   saved['replay_from.receiver'] == old_receiver and saved['completed_through.receiver'] == new_receiver
                   and saved['replay_from.receiver_identity'] != saved['completed_through.receiver_identity'], 'control/')
        self.command('docker', 'kill', first)
        second = self.start_worker(scenario, 'w2')
        replacement = self.generation(scenario, 'w2')
        self.check('Fresh task loads live durable F/W beyond the original capture',
                   json.loads((self.control / (replacement + '.start.json')).read_text())['loaded'] == saved, 'control/')
        for phase in ['legacy', 'history', 'activation', 'migration', 'stream']:
            self.signal(replacement, 'release-' + phase)
        self.signal(replacement, 'permit', 10000)
        self.marker(replacement, 'live-reader')
        before_rollback = prospective.boundary.point(observer[0], self.library)
        exp.execute(transaction, 'rollback')
        self.progressed(self.terminal(observer, before_rollback, transaction[1], 'RB', 'rolled-back'), 'rolled-back')
        self.check('Restart reconstructs open clear and rollback emits no clear or reload', self.inspect(scenario, 'rolled-back') == before_restart, 'control/')
        exp.execute(transaction, 'live-commit-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(transaction, 'live-commit-reload', f'INSERT INTO {self.library}.B VALUES (201)')
        exp.execute(writer, 'live-unrelated-commit', f'INSERT INTO {self.library}.A VALUES (201)')
        self.progressed(prospective.boundary.point(observer[0], self.library)['sequence'], 'second-open')
        before_commit = prospective.boundary.point(observer[0], self.library)
        exp.execute(transaction, 'commit')
        self.progressed(self.terminal(observer, before_commit, transaction[1], 'CM', 'committed'), 'committed')
        final = self.inspect(scenario, 'committed')
        operations = [(r['value']['op'], (r['value'].get('after') or {}).get('ID')) for r in final[len(before_restart):]]
        self.check('Live committed clear preserves unrelated progress and truncate-before-reload', operations == [('c', 201), ('t', None), ('c', 201)], 'control/')
        self.check('Whole live transition and restart have no snapshot records', all(r['value']['op'] != 'r' for r in final), 'control/')
        exp.fixture.verify_rows({'A': [1, 2, 3, 9, 10, 12, 14, 200, 201], 'B': [201], 'M': [1, 2, 3, 21], 'U': [1, 2, 3]})
        self.request(second, scenario, 'PUT', '/stop')
        self.marker(replacement, 'drained')
        self.check('Native source drains before fixture cleanup', (self.control / (replacement + '.stop-requested.json')).exists(), 'control/')
        self.report['status'] = 'passed'
        self.save()

    def cleanup(self):
        # End owned native JVMs before detaching their private source network.
        # The ordinary local cleanup can then verify all labeled networks absent.
        errors = []

        def attempt(action):
            try:
                return action()
            except Exception as error:
                errors.append(self.fixture.redact(str(error)))
                return None

        for worker in self.logs:
            attempt(lambda: self.command('docker', 'kill', worker, check=False))
            attempt(lambda: self.command('docker', 'network', 'disconnect', '--force', self.egress, worker, check=False))
        attempt(lambda: self.command('docker', 'network', 'rm', self.egress, check=False))
        attempt(super().cleanup)
        for process in self.children:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                errors.append('Native worker process did not terminate')
                attempt(process.kill)
                attempt(lambda: process.wait(timeout=5))
            except Exception as error:
                errors.append(self.fixture.redact(str(error)))
        for thread in self.log_threads:
            attempt(lambda: thread.join(timeout=5))
            if thread.is_alive():
                errors.append('Native worker log reader did not terminate')
        attempt(lambda: self.command('docker', 'network', 'rm', self.egress, check=False))
        containers = attempt(lambda: self.command('docker', 'ps', '-aq', '--filter', 'label=dcg.local.probe=' + self.prefix))
        networks = attempt(lambda: self.command('docker', 'network', 'ls', '-q', '--filter', 'label=dcg.local.probe=' + self.prefix))
        if containers is None or networks is None or containers or networks:
            errors.append('Owned worker/container/network absence was not verified')
        self.report['native_worker_cleanup'] = {'status': 'passed' if not errors else 'failed', 'errors': errors}
        if errors:
            self.report['status'] = 'failed'
        self.save()


def main():
    if len(sys.argv) != 3:
        raise SystemExit('Usage: dcg-live-handoff-probe.py NEW_DIRECTORY DCNCdddd; explicit IBMI_TEST_CONFIG required')
    import pyodbc
    pyodbc.pooling = False
    root = Path(sys.argv[1])
    root.mkdir()
    capture = prospective.census.Experiment(root / 'native-boundary', sys.argv[2], 1)
    experiment = None
    try:
        prospective.prepare(capture, discover_history=True)
        capture.persist()
        bundle, replay = build_bundle(capture, root / 'online-handoff.json')
        drain_capture_sessions(capture)
        experiment = LiveExperiment(root / 'connect', capture, bundle, replay)
        experiment.stage()
        experiment.start_broker()
        experiment.observer_container = experiment.container('observer', ['-v', f'{experiment.plugin}:/probe:ro', '-v', f'{experiment.control}:/control',
                                                            '--entrypoint', '/bin/sh', base.IMAGE, '-c', 'sleep 2400'])
        experiment.exercise_native()
    except BaseException as error:
        (root / 'failure.txt').write_text(capture.fixture.redact(traceback.format_exc()))
        capture.report.update(status='failed', error=capture.fixture.redact(str(error)))
        capture.persist()
        if experiment:
            experiment.report.update(status='failed', error=capture.fixture.redact(str(error)))
            if hasattr(experiment, 'observer_container'):
                try:
                    experiment.checkpoint(experiment.observer_container, 'handofflive', 'failure-independent')
                    experiment.inspect('handofflive', 'failure-independent')
                except Exception as diagnostic:
                    experiment.report['failure_evidence_error'] = capture.fixture.redact(str(diagnostic))
            experiment.save()
        raise
    finally:
        cleanup_owned(experiment, capture)
    if (not experiment or experiment.report.get('status') != 'passed'
            or capture.report.get('fixture_cleanup', {}).get('status') != 'passed' or capture.report.get('connection_cleanup_errors')):
        raise SystemExit('Live handoff or cleanup failed; retained evidence identifies the failure')
    print(root / 'connect/report.json', flush=True)


if __name__ == '__main__':
    main()
