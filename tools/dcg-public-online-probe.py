#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Ordinary packaged connector, real legacy offset, public online truncate opt-in.

Explicit fresh isolated IBM i fixture plus owned local Kafka only. Test helpers
are on the independent audit classpath, never on the worker plugin path.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback
import zipfile

sys.dont_write_bytecode = True

def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


automatic = load('automatic_public', 'dcg-automatic-task-probe.py')
packaged = load('packaged_public', 'dcg-packaged-probe.py')
live, base = automatic.live, automatic.base
ROOT = Path(__file__).resolve().parent.parent
CLASS = 'io.debezium.connector.db2as400.As400RpcConnector'
CONTROL = '__debezium-truncate.test'


class Experiment(automatic.Experiment):
    def stage(self):
        super().stage()
        self.audit = self.directory / 'audit'
        self.plugin.rename(self.audit)
        classes = ROOT / 'debezium-connector-ibmi/target/test-classes'
        with zipfile.ZipFile(self.audit / 'packaged-audit.jar', 'w') as jar:
            for path in classes.rglob('As400PackagedProbeAudit*.class'):
                jar.write(path, str(path.relative_to(classes)))
        self.plugin.mkdir()
        distribution_dir = self.plugin / 'ibmi'
        distribution_dir.mkdir()
        distribution = ROOT / 'debezium-connector-ibmi/target/debezium-connector-ibmi-3.8.0-SNAPSHOT-plugin.zip'
        with zipfile.ZipFile(distribution) as archive:
            for item in archive.infolist():
                if item.filename.endswith('.jar'):
                    (distribution_dir / Path(item.filename).name).write_bytes(archive.read(item))
        contents = [name for jar in distribution_dir.glob('*.jar') for name in zipfile.ZipFile(jar).namelist()]
        self.check('Worker plugin contains ordinary packaged task and no test provider',
                   'io/debezium/connector/db2as400/As400ConnectorTask.class' in contents
                   and not any('ProbeConnector' in n or 'As400CheckpointReceipts' in n for n in contents), 'plugin/')
        for check in self.report['checks']:
            if check['name'] == 'candidate artifact excludes custom receipt and owner authority classes':
                check.update(name='Independent audit helpers exclude custom receipt and owner authority', evidence='audit/')
        self.report.update(scope='Public default-off configuration; ordinary plugin/class; actual legacy snapshot and offset before migration',
                           initialization='Ordinary connector snapshot and streaming produce the actual durable offset; no controller seed',
                           transaction_boundary='poll during ordinary legacy startup, connector after online opt-in',
                           migration_enabled=True, default_enabled=False, controller_seeded_offset=False,
                           configured_connector=CLASS, distribution_sha256=hashlib.sha256(distribution.read_bytes()).hexdigest())
        self.report.pop('production_streaming_enabled', None)
        self.report['normal_connector_path_enabled'] = True
        self.report['plugin_sha256'] = {str(p.relative_to(self.plugin)): hashlib.sha256(p.read_bytes()).hexdigest() for p in distribution_dir.iterdir()}
        self.report['harness_sha256']['tools/dcg-public-online-probe.py'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def container(self, name, args):
        config = self.directory / (name + '.properties')
        if not config.exists():
            return base.Experiment.container(self, name, args)
        with config.open('a') as stream:
            stream.write('request.timeout.ms=5000\ndefault.api.timeout.ms=10000\nworker.sync.timeout.ms=20000\nrebalance.timeout.ms=30000\n'
                         'plugin.path=/connector\nconfig.providers=file\nconfig.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider\n')
        worker = base.Experiment.container(self, name, ['--user', f'{os.getuid()}:{os.getgid()}', '--add-host', self.native_host,
                '-v', f'{self.plugin}:/connector:ro', '-v', f'{self.audit}:/probe:ro', '-v', f'{self.audit}:/audit:ro',
                '-v', f'{self.control}:/control', '-v', f'{config}:/worker.properties:ro',
                '--entrypoint', '/bin/sh', base.IMAGE, '-c', 'sleep 2400'])
        self.command('docker', 'network', 'connect', '--gw-priority', '1', self.egress, worker)
        installed = subprocess.run(['docker', 'exec', '-i', worker, 'sh', '-c', 'umask 077; cat > /dev/shm/dcg-credentials.properties'],
                                   input=self.fixture.properties(), text=True, capture_output=True, timeout=10)
        if installed.returncode:
            raise RuntimeError('Cannot install transient worker credentials')
        path = self.directory / (name + '.native.log')
        process = subprocess.Popen(['docker', 'exec', '-e', 'KAFKA_HEAP_OPTS=-Xms128m -Xmx512m',
                                    '-e', 'KAFKA_OPTS=-Dlog4j2.configurationFile=/audit/native-console.properties', worker,
                                    '/opt/kafka/bin/connect-distributed.sh', '/worker.properties'],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.children.append(process)
        def collect():
            with path.open('w') as stream:
                for line in process.stdout:
                    stream.write(self.fixture.redact(line)); stream.flush()
        thread = threading.Thread(target=collect, daemon=True)
        thread.start(); self.log_threads.append(thread); self.logs[worker] = path
        return worker

    def command(self, *args, timeout=45, check=True):
        if args[:2] == ('docker', 'logs') and args[-1] in getattr(self, 'logs', {}):
            return self.logs[args[-1]].read_text()
        result = subprocess.run(list(args), text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        with (self.directory / 'commands.log').open('a') as stream:
            stream.write(self.fixture.redact(json.dumps(list(args)) + '\n' + result.stdout + '\n'))
        if check and result.returncode:
            raise RuntimeError(self.fixture.redact(f'Command failed ({result.returncode}): {args!r}\n{result.stdout[-3000:]}'))
        return result.stdout.strip()

    def failed(self, worker, name, expected):
        status = self.rest(worker, '/connectors/' + name + '/status')
        (self.directory / (name + '-failed-status.json')).write_text(self.fixture.redact(json.dumps(status, indent=2)))
        errors = [t for t in status['tasks'] if t['state'] == 'FAILED']
        if errors and not any(expected in t.get('trace', '') for t in errors):
            raise RuntimeError('Task failed for an unexpected reason; retained ' + name + ' status')
        return bool(errors)

    def observer(self):
        self.observer_container = base.Experiment.container(self, 'observer', ['--user', f'{os.getuid()}:{os.getgid()}',
                '-v', f'{self.audit}:/probe:ro', '-v', f'{self.audit}:/audit:ro', '-v', f'{self.control}:/control',
                '--entrypoint', '/bin/sh', base.IMAGE, '-c', 'sleep 3600'])

    rest_request = packaged.PackagedExperiment.rest_request
    rest = packaged.PackagedExperiment.rest

    def configuration(self, enabled):
        config = {'connector.class': CLASS, 'name': 'automatic', 'tasks.max': '1', 'topic.prefix': 'test',
                  'database.dbname': self.fixture.server_name, 'database.schema': self.library,
                  'table.include.list': ','.join(self.library + '.' + t for t in ['A', 'B', 'M']), 'secure': 'false', 'snapshot.mode': 'initial',
                  'truncate.transactional.enabled': str(enabled).lower(), 'skipped.operations': 'none' if enabled else 't',
                  'max.batch.size': '64', 'max.queue.size': '2048', 'poll.interval.ms': '10', 'heartbeat.interval.ms': '1000',
                  'provide.transaction.metadata': 'false', 'event.converting.failure.handling.mode': 'fail', 'errors.tolerance': 'none',
                  'exactly.once.support': 'requested', 'transaction.boundary': 'connector' if enabled else 'poll',
                  'dcg.retention': 'operator-managed', 'dcg.journal': self.library + '/QSQJRN',
                  'schema.history.internal.kafka.bootstrap.servers': 'broker:9092', 'schema.history.internal.kafka.topic': 'bindings-automatic',
                  'producer.override.bootstrap.servers': 'broker:9092', 'admin.override.bootstrap.servers': 'broker:9092',
                  'tombstones.on.delete': 'true', 'topic.naming.strategy': 'io.debezium.schema.SchemaTopicNamingStrategy',
                  'decimal.handling.mode': 'precise', 'binary.handling.mode': 'bytes', 'time.precision.mode': 'adaptive_time_microseconds',
                  'schema.name.adjustment.mode': 'none', 'field.name.adjustment.mode': 'none', 'trim.non.xml.charsequence.field.mode': 'none',
                  'key.converter': 'org.apache.kafka.connect.json.JsonConverter', 'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
                  'key.converter.schemas.enable': 'false', 'value.converter.schemas.enable': 'false'}
        for key in ['database.hostname', 'database.user', 'database.password']:
            config[key] = '${file:/dev/shm/dcg-credentials.properties:' + key + '}'
        return config

    def setup_connector(self, worker):
        for topic in [f'test.{self.fixture.server_name}.{self.library}.{t}' for t in ['A', 'B', 'M']] + [CONTROL, 'bindings-automatic', '__debezium-heartbeat.test']:
            extra = ['--config', 'cleanup.policy=delete', '--config', 'retention.ms=-1'] if topic == 'bindings-automatic' else []
            self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                         '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1', *extra)
        config = self.configuration(False)
        (self.directory / 'legacy-connector.json').write_text(json.dumps(config, indent=2))
        (self.control / 'automatic.owned-fixture').write_text(self.library)
        self.rest(worker, '/connectors', {'name': 'automatic', 'config': config})
        def snapshot_complete():
            status = self.rest(worker, '/connectors/automatic/status')
            (self.directory / 'legacy-status.json').write_text(self.fixture.redact(json.dumps(status, indent=2)))
            if any(t['state'] == 'FAILED' for t in status['tasks']):
                raise RuntimeError('Ordinary legacy startup failed; retained worker log and status')
            return all(len(self.table_records(t, 'snapshot-' + t)) == 3 for t in ['A', 'B', 'M'])
        base.wait_for('legacy snapshot', snapshot_complete, 150)
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (10)', 'legacy-insert.log', True)
        row = self.inserted('A', 10, 'legacy-insert')
        def completed():
            offset = self.checkpoint(self.observer_container, 'automatic', 'legacy')
            return offset if offset and offset.get('snapshot_completed') == 'true' and offset.get('offset.processed') == 'true' and int(offset['offset.event_sequence']) >= int(row['value']['source']['sequence']) else None
        saved = base.wait_for('real completed legacy offset', completed, 90)
        self.check('Ordinary source published snapshots and a streaming row with its real durable offset', 'offset.version' not in saved, 'control/')
        self.request(worker, 'automatic', 'PUT', '/stop')
        base.wait_for('legacy stopped', lambda: self.rest(worker, '/connectors/automatic/status')['connector']['state'] == 'STOPPED', 60)
        return {t: self.table_records(t, 'before-migration-' + t) for t in ['A', 'B', 'M']}

    def lifecycle(self, worker, phase):
        timeout = 360 if phase == 'handoff-complete' else 180
        started = time.monotonic()
        result = base.wait_for(phase, lambda: 'Transactional truncate test: ' + phase in self.logs[worker].read_text(), timeout)
        self.report.setdefault('phase_waits', []).append({'phase': phase, 'seconds': time.monotonic() - started, 'limit_seconds': timeout})
        self.save()
        return result

    def raw_interval(self, start, label):
        def observe(session):
            end = live.prospective.boundary.point(session[0], self.library)
            params = [self.library, 'QSQJRN', start['library'], start['receiver'], start['sequence'],
                      end['library'], end['receiver'], end['sequence'], start['sequence']]
            sql = live.prospective.boundary.AUDIT_SQL
            rows = live.prospective.census.rows(session[0], sql, params, 2000)
            live.prospective.census.save(self.directory / (label + '-raw.json'), {'sql': sql, 'parameters': params, 'rows': rows})
            if any(r['DATA_LENGTH'] > 16000 for r in rows):
                raise ValueError('Native audit payload truncated')
            return rows
        return self.read_native(observe)

    def exercise(self):
        exp = self.capture
        exp.deadline = None
        first = self.start_worker('automatic', 'w1')
        baseline = self.setup_connector(first)
        self.audit_start = self.point()
        busy = exp.connection(transaction=True)
        exp.execute(busy, 'busy-before-enable', f'INSERT INTO {self.library}.U VALUES (90)')
        config = self.configuration(True)
        (self.directory / 'online-connector.json').write_text(json.dumps(config, indent=2))
        self.request(first, 'automatic', 'PUT', '/config', config)
        self.request(first, 'automatic', 'PUT', '/resume')
        writer = exp.connection()
        for key in range(1000, 1010):
            exp.execute(writer, 'busy-writer', f'INSERT INTO {self.library}.A VALUES ({key})')
        rows = self.writer_output(list(range(1000, 1010)), 'busy-prefix')
        self.check('Busy boundary leaves ordinary CDC progressing without prematurely enabling clears',
                   len(rows) == 10 and 'offset.version' not in self.checkpoint(self.observer_container, 'automatic', 'busy'), 'control/')
        exp.execute(busy, 'rollback'); self.close_session(busy)
        self.lifecycle(first, 'handoff-complete')
        saved = self.row_progress(rows[-1], 'migrated')
        self.check('Online admission exposes durable F/W after exact acknowledged legacy handoff',
                   saved.get('offset.version') == '2', 'control/')
        self.check('Enabling truncate adds no snapshot or duplicate legacy records',
                   all(sum(r['value'] is not None and r['value']['op'] == 'r' for r in self.table_records(t, 'after-migration-' + t)) == 3 for t in baseline)
                   and [r['key']['ID'] for r in self.writer_output(list(range(1000, 1010)), 'prefix-final')] == list(range(1000, 1010)), 'control/')
        transaction = exp.connection(transaction=True)
        start = self.point()
        exp.execute(transaction, 'open-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(transaction, 'open-reload', f'INSERT INTO {self.library}.B VALUES (200)')
        old, new = self.fixture.rotate()
        exp.execute(writer, 'unrelated-open', f'INSERT INTO {self.library}.A VALUES (200)')
        saved = self.row_progress(self.inserted('A', 200, 'open'), 'open')
        self.check('Unrelated CDC progresses across receiver rotation while clear is open',
                   saved['replay_from.receiver'] == old and saved['completed_through.receiver'] == new
                   and self.table_records('B', 'open') == baseline['B'], 'control/')
        history = self.records(self.observer_container, 'bindings-automatic', 'history-before-crash')
        self.command('docker', 'kill', first)
        second = self.start_worker('automatic', 'w2')
        self.lifecycle(second, 'recovered')
        self.check('Fresh normal task resumes durable history without a new native boundary',
                   self.records(self.observer_container, 'bindings-automatic', 'history-after-crash') == history, 'control/')
        before = self.point(); exp.execute(transaction, 'rollback')
        self.progressed(self.terminal(before, transaction[1], 'RB', 'rollback'), 'rollback')
        self.check('Restarted open truncate rolls back without publishing clear or reload', self.table_records('B', 'rollback') == baseline['B'], 'control/')
        exp.execute(transaction, 'committed-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(transaction, 'committed-reload', f'INSERT INTO {self.library}.B VALUES (201)')
        before = self.point(); exp.execute(transaction, 'commit')
        self.progressed(self.terminal(before, transaction[1], 'CM', 'commit'), 'commit')
        self.check('Committed truncate precedes reload', [r['value']['op'] for r in self.table_records('B', 'commit')[3:]] == ['t', 'c'], 'control/')
        # A different unchanged file remains eligible for IBM i's optimized DELETE.
        # Confirm the actual D/CG subtype independently; row DELETE is not a substitute.
        for outcome in ['rollback', 'commit']:
            exp.execute(transaction, 'optimized-' + outcome, f'DELETE FROM {self.library}.M')
            exp.execute(transaction, 'mixed-reload-' + outcome, f"INSERT INTO {self.library}.M VALUES (30,12,123.45,1234.567,'ABC','Ä![]',BX'00010203',BX'00FF',9007199254740993,1.25,-2.5)")
            before = self.point(); exp.execute(transaction, outcome)
            self.progressed(self.terminal(before, transaction[1], 'RB' if outcome == 'rollback' else 'CM', 'delete-' + outcome), 'delete-' + outcome)
            output = self.table_records('M', 'delete-' + outcome)
            self.check('Optimized DELETE ' + outcome + ' obeys transaction outcome',
                       output == baseline['M'] if outcome == 'rollback' else [r['value']['op'] for r in output[3:]] == ['t', 'c'], 'control/')
        self.fixture.sql(f"UPDATE {self.library}.M SET S=-123,N=-12.34,D=0.001,C='XYZ',V=NULL,B=BX'FF00807F',VB=BX'1000FF',L=-9007199254740993,F=-1.5,G=3.25 WHERE ID=30", 'mixed-update.log', True)
        def updated():
            rows = self.table_records('M', 'mixed-update')
            return rows[-1] if rows[-1]['value']['op'] == 'u' else None
        update = base.wait_for('mixed update', updated, 90)
        mixed = packaged.continuous.ContinuousExperiment
        self.check('Native mixed schema preserves keys, decimals, text, binary, nulls and 64-bit values',
                   update['key'] == {'ID': 30} and mixed.decode_mixed(update['value']['before']) == mixed.expected_mixed(30)
                   and mixed.decode_mixed(update['value']['after']) == mixed.expected_mixed(30, True), 'control/')
        self.row_progress(update, 'mixed-update')
        raw = self.raw_interval(start, 'transactions')
        clears = [r for r in raw if r['JOURNAL_CODE'] == 'D' and r['JOURNAL_ENTRY_TYPE'] == 'CG']
        subtypes = [bytes.fromhex(r['DATA_HEX'])[108:109].decode('cp037') for r in clears]
        self.check('Independent raw journal confirms subtype 8 TRUNCATE and subtype 3 optimized DELETE', subtypes.count('8') == 2 and subtypes.count('3') == 2, 'transactions-raw.json')
        self.report['raw_clear_subtypes'] = subtypes
        self.close_session(transaction); self.close_session(writer)
        self.request(second, 'automatic', 'PUT', '/stop')
        self.lifecycle(second, 'drained')
        self.check('Normal connector terminal stop completes native drain', True, self.logs[second].name)
        before_missing = {t: self.table_records(t, 'before-missing-' + t) for t in ['A', 'B', 'M']}
        history = self.records(self.observer_container, 'bindings-automatic', 'before-missing-history')
        missing = self.configuration(True)
        missing['name'] = 'missing'
        self.rest(second, '/connectors', {'name': 'missing', 'config': missing})
        base.wait_for('missing-offset rejection', lambda: self.failed(second, 'missing', 'Missing offsets cannot select automatic migration'), 60)
        self.check('Missing offset with existing binding history rejects without output or history changes',
                   all(self.table_records(t, 'missing-' + t) == rows for t, rows in before_missing.items())
                   and self.records(self.observer_container, 'bindings-automatic', 'missing-history') == history, 'control/')
        self.request(second, 'missing', 'PUT', '/stop')
        self.request(second, 'automatic', 'PUT', '/config', self.configuration(False))
        self.request(second, 'automatic', 'PUT', '/resume')
        base.wait_for('unsafe downgrade rejection', lambda: self.failed(second, 'automatic', 'Versioned transaction offsets require buffered recovery'), 60)
        self.check('Disabling transactional truncate cannot reinterpret durable F/W as legacy offsets',
                   all(self.table_records(t, 'downgrade-' + t) == rows for t, rows in before_missing.items())
                   and self.records(self.observer_container, 'bindings-automatic', 'downgrade-history') == history, 'control/')
        self.request(second, 'automatic', 'PUT', '/stop')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 200, *range(1000, 1010)], 'B': [201], 'M': [30], 'U': [1, 2, 3]})
        self.report['status'] = 'passed'; self.save()

    def retain_failure_evidence(self):
        if not hasattr(self, 'observer_container'):
            return
        errors = []
        for topic in ['offsets-automatic', 'bindings-automatic', CONTROL] + [f'test.{self.fixture.server_name}.{self.library}.{t}' for t in ['A', 'B', 'M']]:
            try:
                self.records(self.observer_container, topic, 'failure-' + topic)
            except Exception as error:
                errors.append(self.fixture.redact(str(error)))
        if hasattr(self, 'audit_start'):
            try:
                self.raw_interval(self.audit_start, 'failure')
            except Exception as error:
                errors.append(self.fixture.redact(str(error)))
        self.report['independent_failure_evidence'] = {'errors': errors}; self.save()


def main():
    if len(sys.argv) != 3:
        raise SystemExit('Usage: dcg-public-online-probe.py NEW_DIRECTORY DCNCdddd; isolated IBM i writes and local Kafka')
    import pyodbc
    pyodbc.pooling = False
    root = Path(sys.argv[1]).resolve(); root.mkdir(exist_ok=False)
    with zipfile.ZipFile(root / 'source-at-start.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
        names = subprocess.check_output(['git', '-C', str(ROOT), 'ls-files', '--cached', '--others', '--exclude-standard', '-z']).decode().split('\0')
        for name in sorted(set(names)):
            path = ROOT / name
            if name and path.is_file() and '__pycache__' not in name:
                archive.write(path, name)
    capture = live.prospective.census.Experiment(root / 'native', sys.argv[2], 1)
    capture.fixture.__class__ = packaged.continuous.MixedFixture
    experiment = None
    try:
        capture.fixture.setup()
        experiment = Experiment(root / 'connect', capture)
        experiment.stage(); experiment.start_broker(); experiment.observer(); experiment.exercise()
        capture.report.update(status='passed', source_connected=True, publication='Ordinary packaged plugin with public opt-in')
    except BaseException as error:
        (root / 'failure.txt').write_text(capture.fixture.redact(traceback.format_exc()))
        capture.report.update(status='failed', error=capture.fixture.redact(str(error)))
        if experiment:
            experiment.report.update(status='failed', error=capture.fixture.redact(str(error)))
            experiment.save(); experiment.retain_failure_evidence()
    finally:
        live.cleanup_owned(experiment, capture)
    if not experiment or experiment.report.get('status') != 'passed' or capture.report.get('fixture_cleanup', {}).get('status') != 'passed':
        raise SystemExit('Public online validation failed; evidence and cleanup retained')
    print(experiment.directory / 'report.json', flush=True)


if __name__ == '__main__':
    main()
