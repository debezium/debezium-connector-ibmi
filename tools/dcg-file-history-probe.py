#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Shipping IBM i connector in the real Engine: file history/offsets, no Kafka broker.

Reuses the owned native fixture, transaction ledger and independent raw journal
audit. Requires explicit IBMI_TEST_CONFIG and a fresh DCNCdddd library.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('file_history_public', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)
census = public.live.prospective.census
CLASS = 'io.debezium.connector.db2as400.As400FileHistoryProbe'


class Experiment:
    close_session = public.automatic.Experiment.close_session
    read_native = public.automatic.Experiment.read_native
    point = public.automatic.Experiment.point
    raw_interval = public.Experiment.raw_interval

    def __init__(self, directory, library):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(exist_ok=False)
        self.capture = census.Experiment(self.directory / 'native', library, 1)
        self.fixture = self.capture.fixture
        self.fixture.__class__ = public.packaged.continuous.MixedFixture
        self.library = library
        self.children, self.logs, self.configs = [], [], []
        self.active = None
        self.report = {'status': 'preparing', 'checks': [], 'runtime': 'Actual Debezium Engine, public As400RpcConnector',
                       'offsets': 'FileOffsetBackingStore', 'history': 'FileSchemaHistory with strict binding adapter',
                       'broker': 'none', 'delivery': 'ordered at least once; no broker fencing', 'synthetic_offset': False,
                       'test_native_provider': False}

    def save(self):
        (self.directory / 'report.json').write_text(json.dumps(self.report, indent=2) + '\n')

    def check(self, name, condition):
        self.report['checks'].append({'name': name, 'status': 'passed' if condition else 'failed'})
        self.save()
        if not condition:
            raise AssertionError(name)
        print('PASS ' + name, flush=True)

    def stage(self):
        plugin = self.directory / 'plugin'
        plugin.mkdir()
        archive = ROOT / 'debezium-connector-ibmi/target/debezium-connector-ibmi-3.8.0-SNAPSHOT-plugin.zip'
        with zipfile.ZipFile(archive) as distribution:
            for name in distribution.namelist():
                if name.endswith('.jar'):
                    (plugin / Path(name).name).write_bytes(distribution.read(name))
        contents = [name for p in plugin.glob('*.jar') for name in zipfile.ZipFile(p).namelist()]
        self.check('Shipping plugin contains the file history adapter and no test task/provider',
                   'io/debezium/connector/db2as400/As400FileBindingHistory.class' in contents
                   and not any('ProbeConnector' in name or 'As400FileHistoryProbe' in name for name in contents))
        runtime = self.directory / 'runtime'
        runtime.mkdir()
        for name in (ROOT / 'debezium-connector-ibmi/target/connect-probe-classpath.txt').read_text().strip().split(os.pathsep):
            source = Path(name)
            if source.name.endswith('-tests.jar') or (plugin / source.name).exists():
                continue
            shutil.copy2(source, runtime / source.name)
        with zipfile.ZipFile(self.directory / 'consumer.jar', 'w') as jar:
            classes = ROOT / 'debezium-connector-ibmi/target/test-classes'
            for file in classes.rglob('As400FileHistoryProbe*.class'):
                jar.write(file, str(file.relative_to(classes)))
        self.classpath = os.pathsep.join(map(str, [self.directory / 'consumer.jar', plugin / '*', runtime / '*']))
        self.java = census.native.java_executable()
        logging = self.directory / 'logback.xml'
        logging.write_text('<configuration><appender name="console" class="ch.qos.logback.core.ConsoleAppender">'
                           '<encoder><pattern>%date %-5level %logger - %msg%n</pattern></encoder></appender>'
                           '<root level="INFO"><appender-ref ref="console"/></root></configuration>')
        self.command = [self.java, '-Xmx384m', '-Duser.timezone=UTC', '-Dlogback.configurationFile=' + str(logging), '-cp', self.classpath, CLASS]
        self.report['distribution_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
        self.report['classpath_sha256'] = {str(p.relative_to(self.directory)): hashlib.sha256(p.read_bytes()).hexdigest()
                                          for folder in [plugin, runtime] for p in folder.glob('*.jar')}
        with zipfile.ZipFile(self.directory / 'source-at-start.zip', 'w', zipfile.ZIP_DEFLATED) as saved:
            files = subprocess.check_output(['git', '-C', str(ROOT), 'ls-files', '--cached', '--others', '--exclude-standard', '-z']).decode().split('\0')
            for name in sorted(set(files)):
                if name and (ROOT / name).is_file() and '__pycache__' not in name:
                    saved.write(ROOT / name, name)
        self.save()

    def configuration(self, enabled):
        # Public configuration shared with the Kafka probe, then ordinary Engine stores.
        profile = public.Experiment.__new__(public.Experiment)
        profile.fixture, profile.library, profile.source_offsets = self.fixture, self.library, True
        values = profile.configuration(enabled)
        values = {k: v for k, v in values.items() if not k.startswith(('schema.history.internal.kafka.', 'producer.override.', 'admin.override.'))
                  and k not in ['database.hostname', 'database.user', 'database.password', 'database.dbname']}
        values.update({'name': 'file-history-native', 'offset.storage': 'org.apache.kafka.connect.storage.FileOffsetBackingStore',
                       'offset.storage.file.filename': str(self.directory / 'offsets.dat'), 'offset.flush.interval.ms': '0',
                       'schema.history.internal': 'io.debezium.storage.file.history.FileSchemaHistory',
                       'schema.history.internal.file.filename': str(self.directory / 'history.jsonl'),
                       'record.processing.order': 'ORDERED', 'record.processing.threads': '1'})
        return values

    def start(self, name, enabled):
        if self.active is not None and self.active[1].poll() is None:
            raise RuntimeError('Previous Engine must stop before starting a replacement')
        config = self.configuration(enabled)
        (self.directory / (name + '-configuration.json')).write_text(json.dumps(config, indent=2) + '\n')
        # mkstemp creates an owner-only file and honors the caller's TMPDIR.
        fd, filename = tempfile.mkstemp(prefix='ibmi-file-probe-', suffix='.properties')
        path = Path(filename)
        self.configs.append(path)
        with os.fdopen(fd, 'w') as stream:
            stream.write(self.fixture.properties())
            for key, value in config.items():
                stream.write(key + '=' + ''.join('\\u%04x' % ord(c) for c in str(value)) + '\n')
        process = subprocess.Popen(self.command + ['run', str(path), str(self.directory / name)], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        self.children.append((name, process, path))
        self.active = self.children[-1]
        def collect():
            with (self.directory / (name + '.log')).open('w') as stream:
                for line in process.stdout:
                    stream.write(self.fixture.redact(line)); stream.flush()
        reader = threading.Thread(target=collect, daemon=True)
        reader.start(); self.logs.append(reader)

    def stop(self, kill=False):
        if self.active is None:
            return
        name, process, _ = self.active
        if process.poll() is None:
            if kill:
                process.kill()
            else:
                (self.directory / name / 'stop').touch()
            process.wait(timeout=60)
        if not kill and process.returncode != 0:
            raise RuntimeError('Engine failed; inspect ' + name + '.log')

    def wait(self, label, predicate, timeout=150):
        def checked():
            if self.active is not None and self.active[1].poll() is not None:
                raise RuntimeError('Engine stopped while waiting for ' + label)
            return predicate()
        return public.base.wait_for(label, checked, timeout)

    def records(self, table=None):
        result = []
        for name, _, _ in self.children:
            file = self.directory / name / 'records.jsonl'
            if not file.exists():
                continue
            data = file.read_text()
            for line in data.splitlines(keepends=True):
                if not line.endswith('\n'):
                    continue  # A killed consumer can leave only an unacknowledged partial final line.
                row = json.loads(line)
                if table is None or row['topic'] == f'test.{self.fixture.server_name}.{self.library}.{table}':
                    result.append(row)
        return result

    def offset(self):
        result = subprocess.run(self.command + ['offset', str(self.active[2]), str(self.directory)], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
        if result.returncode:
            (self.directory / 'offset-read-failure.log').write_text(self.fixture.redact(result.stdout))
            raise RuntimeError('Independent file-offset read failed')
        return json.loads((self.directory / 'offset-read.json').read_text())

    def row(self, table, key):
        return self.wait('insert ' + str(key), lambda: next((r for r in self.records(table)
                        if r['value'] and r['value']['op'] == 'c' and r['key'] == {'ID': key}), None))

    def progressed(self, row):
        sequence = int(row['value']['source']['sequence'])
        return self.wait('durable native offset', lambda: (saved if int(saved.get('completed_through.sequence', '0')) >= sequence else None)
                         if (saved := self.offset()) else None)

    def run(self):
        self.fixture.setup()
        self.start('legacy', False)
        self.wait('ordinary snapshot', lambda: all(len(self.records(t)) == 3 for t in ['A', 'B', 'M']))
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (10)', 'legacy-insert.log', True)
        legacy_row = self.row('A', 10)
        self.wait('legacy offset', lambda: saved.get('offset.processed') == 'true' and saved.get('snapshot_completed') == 'true'
                  and int(saved.get('offset.event_sequence', '0')) >= int(legacy_row['value']['source']['sequence'])
                  if (saved := self.offset()) else False)
        self.stop()
        self.check('Ordinary public connector produced its own snapshot and file offsets',
                   'offset.version' not in self.offset() and not (self.directory / 'history.jsonl').exists())
        self.start('online', True)
        self.wait('public file-history handoff', lambda: self.offset().get('offset.version') == '2', 360)
        self.check('Online handoff uses durable file history without another snapshot',
                   (self.directory / 'history.jsonl').stat().st_size > 0
                   and all(sum(r['value'] is not None and r['value']['op'] == 'r' for r in self.records(t)) == 3 for t in ['A', 'B', 'M']))
        transaction = self.capture.connection(transaction=True)
        audit = self.point()
        self.capture.execute(transaction, 'clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        self.capture.execute(transaction, 'reload', f'INSERT INTO {self.library}.B VALUES (200)')
        old, new = self.fixture.rotate()
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (20)', 'unrelated.log', True)
        saved = self.progressed(self.row('A', 20))
        self.check('Unrelated CDC progresses across receiver rotation while clear stays unpublished',
                   saved['replay_from.receiver'] == old and saved['completed_through.receiver'] == new and len(self.records('B')) == 3)
        history = (self.directory / 'history.jsonl').read_bytes()
        self.stop(kill=True)
        self.start('replacement', True)
        self.wait('fresh recovery', lambda: 'Transactional truncate test: recovered' in (self.directory / 'replacement.log').read_text(), 120)
        self.check('Replacement recovers the same durable file history and F/W',
                   (self.directory / 'history.jsonl').read_bytes() == history)
        self.capture.execute(transaction, 'rollback')
        self.fixture.sql(f'INSERT INTO {self.library}.A VALUES (21)', 'after-rollback.log', True)
        self.progressed(self.row('A', 21))
        self.check('Rollback after restart emits neither truncate nor reload', len(self.records('B')) == 3)
        self.capture.execute(transaction, 'clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        self.capture.execute(transaction, 'reload', f'INSERT INTO {self.library}.B VALUES (201)')
        self.capture.execute(transaction, 'commit')
        self.progressed(self.row('B', 201))
        self.check('Committed truncate precedes its reload', [r['value']['op'] for r in self.records('B')[3:]] == ['t', 'c'])
        baseline = self.records('M')
        for outcome, key in [('rollback', 22), ('commit', 23)]:
            self.capture.execute(transaction, 'optimized-delete', f'DELETE FROM {self.library}.M')
            self.capture.execute(transaction, 'mixed-reload', f"INSERT INTO {self.library}.M VALUES (30,12,123.45,1234.567,'ABC','Ä![]',BX'00010203',BX'00FF',9007199254740993,1.25,-2.5)")
            self.capture.execute(transaction, outcome)
            self.fixture.sql(f'INSERT INTO {self.library}.A VALUES ({key})', 'after-delete-' + outcome + '.log', True)
            self.progressed(self.row('A', key))
            rows = self.records('M')
            self.check('Optimized DELETE ' + outcome + ' preserves its outcome and ordering',
                       rows == baseline if outcome == 'rollback' else [r['value']['op'] for r in rows[3:]] == ['t', 'c'])
        mixed = public.packaged.continuous.ContinuousExperiment
        self.check('Native mixed-column schema survives file-history restart',
                   mixed.decode_mixed(self.records('M')[-1]['value']['after']) == mixed.expected_mixed(30))
        raw = self.raw_interval(audit, 'transactions')
        subtypes = [bytes.fromhex(r['DATA_HEX'])[108:109].decode('cp037') for r in raw
                    if r['JOURNAL_CODE'] == 'D' and r['JOURNAL_ENTRY_TYPE'] == 'CG']
        self.report['raw_clear_subtypes'] = subtypes
        self.check('Independent raw journal confirms both transactional clear subtypes', subtypes.count('8') == 2 and subtypes.count('3') == 2)
        self.stop()
        before = self.records()
        offsets = (self.directory / 'offsets.dat').read_bytes()
        (self.directory / 'history.jsonl').rename(self.directory / 'retained-history.jsonl')
        self.start('missing-history', True)
        self.active[1].wait(timeout=60)
        self.check('Missing history rejects before output and preserves the real offset file',
                   self.active[1].returncode != 0 and self.records() == before
                   and (self.directory / 'offsets.dat').read_bytes() == offsets and not (self.directory / 'history.jsonl').exists()
                   and 'Required binding history is absent' in (self.directory / 'missing-history.log').read_text())
        (self.directory / 'retained-history.jsonl').rename(self.directory / 'history.jsonl')
        self.fixture.verify_rows({'A': [1, 2, 3, 10, 20, 21, 22, 23], 'B': [201], 'M': [30], 'U': [1, 2, 3]})
        self.report['status'] = 'passed'
        self.save()

    def cleanup(self):
        errors = []
        for name, process, _ in self.children:
            if process.poll() is None:
                output = self.directory / name
                if output.exists():
                    (output / 'stop').touch()
                else:
                    process.terminate()
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    process.kill()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        errors.append('Process did not exit: ' + name)
                    errors.append('Forced termination during cleanup: ' + name)
        for reader in self.logs:
            reader.join(timeout=10)
            if reader.is_alive():
                errors.append('Process log reader did not drain')
        for path in self.configs:
            path.unlink(missing_ok=True)
        self.report['process_cleanup'] = {'status': 'passed' if not errors else 'failed', 'errors': errors,
                                          'temporary_properties_absent': all(not p.exists() for p in self.configs)}
        self.save()
        # Native cleanup independently checks pending transactions and all owned
        # file/journal locks before any drop. Never infer lock release from exit.
        if any(process.poll() is None for _, process, _ in self.children):
            public.live.drain_capture_sessions(self.capture)
            self.capture.report['fixture_cleanup'] = {'status': 'blocked', 'error': 'Engine process remains alive; no fixture deletion authorized'}
            self.capture.persist()
        else:
            self.capture.cleanup()


def main():
    if len(sys.argv) != 3:
        raise SystemExit('Usage: dcg-file-history-probe.py NEW_DIRECTORY DCNCdddd; explicit isolated IBM i writes; no Kafka broker')
    import pyodbc
    pyodbc.pooling = False
    experiment = Experiment(sys.argv[1], sys.argv[2])
    try:
        experiment.stage()
        experiment.run()
    except BaseException as error:
        experiment.report.update(status='failed', error=experiment.fixture.redact(str(error)))
        (experiment.directory / 'failure.txt').write_text(experiment.fixture.redact(traceback.format_exc()))
    finally:
        experiment.cleanup()
    if (experiment.report['status'] != 'passed' or experiment.capture.report.get('fixture_cleanup', {}).get('status') != 'passed'
            or experiment.report.get('process_cleanup', {}).get('status') != 'passed'):
        raise SystemExit('File-history native experiment failed; retained evidence and cleanup results')
    print(experiment.directory / 'report.json', flush=True)


if __name__ == '__main__':
    main()
