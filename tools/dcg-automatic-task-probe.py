# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Explicit isolated native automatic-boundary task + local Kafka experiment.

Only an ordinary legacy offset is seeded. No boundary bundle, candidate F/W,
native object identity or schema is supplied to the task by this controller.
"""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import threading
import traceback
import zipfile

sys.dont_write_bytecode = True
SPEC = importlib.util.spec_from_file_location('live', Path(__file__).with_name('dcg-live-handoff-probe.py'))
live = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live)
base = live.base
CLASS = 'io.debezium.connector.db2as400.As400AutomaticHandoffProbeConnector'
live.CLASS = CLASS


class Experiment(live.LiveExperiment):
    def __init__(self, directory, capture, fault='none'):
        live.handoff.barrier.BarrierExperiment.__init__(self, directory)
        self.capture, self.fixture, self.library = capture, capture.fixture, capture.library
        self.egress = self.prefix + '-native'
        self.children, self.log_threads, self.logs = [], [], {}
        self.writer_actor = None
        self.writer_stop = None
        if fault not in ['none', 'pending', 'barrier', 'migration']:
            raise ValueError('Unknown automatic task fault')
        self.fault = fault
        addresses = {r[4][0] for r in socket.getaddrinfo(self.fixture.target.host, None, socket.AF_INET, socket.SOCK_STREAM)}
        if len(addresses) != 1:
            raise ValueError('Exact isolated native address required')
        self.native_host = self.fixture.target.host + ':' + addresses.pop()

    def stage(self):
        live.handoff.barrier.BarrierExperiment.stage(self)
        classes = live.handoff.ROOT / 'debezium-connector-ibmi/target/test-classes'
        with zipfile.ZipFile(self.plugin / 'synthetic-probe.jar', 'a') as jar:
            for path in classes.rglob('As400AutomaticHandoffProbeConnector*.class'):
                jar.write(path, str(path.relative_to(classes)))
        (self.plugin / 'native-console.properties').write_text('\n'.join([
            'status=warn', 'name=AutomaticNativeConsole', 'appender.console.type=Console', 'appender.console.name=Console',
            'appender.console.layout.type=PatternLayout', 'appender.console.layout.pattern=%d{ISO8601} %-5p %c - %m%n',
            'rootLogger.level=info', 'rootLogger.appenderRef.console.ref=Console', '']))
        self.report.update(scope='Actual normal task owns native boundary, legacy dispatch, durable history/offsets and continuation',
                           source_connected=True, migration_enabled=False, transaction_metadata='disabled',
                           initialization='Ordinary source offset seeded only; no supplied boundary, bindings or F/W',
                           native_proof='Fresh isolated fixture; operator retention and fixed schema obligations remain')
        self.report['fault'] = self.fault
        self.report['binding_authority'] = 'Version-5 immutable candidates selected by the fenced Connect checkpoint; no activation-history append'
        self.report['plugin_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.plugin.iterdir()}
        self.report['harness_sha256'][str(Path(__file__).relative_to(live.handoff.ROOT))] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        self.save()

    def rest(self, worker, endpoint, payload=None, tolerate=False):
        return live.handoff.barrier.BarrierExperiment.rest(self, worker, endpoint, payload, tolerate=tolerate)

    def progressed(self, sequence, label):
        self.report.setdefault('checkpoint_targets', {})[label] = str(sequence)
        self.save()
        def reached():
            value = self.checkpoint(self.observer_container, 'automatic', label)
            return value if value and int(value.get('completed_through.sequence', 0)) >= int(sequence) else None
        return base.wait_for(label, reached, 150)

    def task_marker(self, name, worker=None):
        prefix = 'automatic-' + (worker or self.active_role) + '-*/'

        def found():
            result = next(iter(self.control.glob(prefix + name + '.json')), None)
            if not result and name != 'drained' and list(self.control.glob(prefix + 'drained.json')):
                raise RuntimeError('Native task drained before ' + name + '; see retained worker log')
            return result

        return base.wait_for(name, found, 240)

    def start_worker(self, scenario, role):
        self.active_role = scenario + '-' + role
        return super().start_worker(scenario, role)

    def table_records(self, table, label):
        return self.records(self.observer_container, f'test.{self.fixture.server_name}.{self.library}.{table}', label)

    def row_progress(self, record, label):
        source = record['value']['source']
        sequence = source['sequence']
        if (source['receiver_library'] != self.library or not source['receiver']
                or not isinstance(sequence, str) or not sequence.isdigit() or int(sequence) <= 0):
            raise ValueError('Exact native output coordinate required')
        return self.progressed(sequence, label)

    def writer_output(self, progress, label):
        def committed():
            rows = [r for r in self.table_records('A', label) if r['key']['ID'] >= 1000]
            return rows if rows and [r['value']['after']['ID'] for r in rows] == progress else None
        return base.wait_for(label, committed, 150)

    def inserted(self, table, key, label):
        def committed():
            rows = [r for r in self.table_records(table, label)
                    if r['value'] and r['value']['op'] == 'c' and r['key']['ID'] == key]
            if len(rows) > 1:
                raise ValueError('Duplicate fixture insert output')
            return rows[0] if rows else None
        return base.wait_for(label, committed, 150)

    def close_session(self, session):
        session[0].close()
        self.capture.connections.remove(session[0])

    def read_native(self, action):
        # Observations carry no transaction or session state across worker restarts.
        # Do not retry a failed read or reconnect an in-flight mutation session.
        session = self.capture.connection(read_only=True)
        try:
            return action(session)
        finally:
            self.close_session(session)

    def point(self):
        return self.read_native(lambda session: live.prospective.boundary.point(session[0], self.library))

    def terminal(self, start, job, entry_type, label):
        return self.read_native(lambda session: super(Experiment, self).terminal(session, start, job, entry_type, label))

    def retain_writer_tail(self, record):
        source = record['value']['source']

        def observe(session):
            end = live.prospective.boundary.point(session[0], self.library)
            start = int(source['sequence'])
            if source['receiver_library'] != self.library or source['receiver'] != end['receiver'] or not 0 < start <= end['sequence']:
                raise ValueError('Exact single-receiver fixture tail required')
            params = [self.library, 'QSQJRN', self.library, source['receiver'], start,
                      self.library, end['receiver'], end['sequence'], start]
            sql = live.prospective.boundary.AUDIT_SQL
            rows = live.prospective.census.rows(session[0], sql, params, 2000)
            live.prospective.census.save(self.directory / 'writer-tail-native.json',
                                        {'sql': sql, 'parameters': params, 'last_output_source': source, 'observed_tail': end, 'rows': rows})
        self.read_native(observe)

    def create_automatic(self, worker, anchor):
        config = {'connector.class': CLASS, 'name': 'automatic', 'tasks.max': '1', 'topic.prefix': 'test',
                  'database.hostname': 'stdin-automatic-handoff.invalid', 'database.user': 'fixture', 'database.password': 'fixture',
                  'database.dbname': self.fixture.server_name, 'database.schema': self.library, 'table.include.list': self.library + r'\.(A|B|M)',
                  'secure': 'false',  # This isolated server uses the same non-TLS JT400 transport as the earlier native probes.
                  'skipped.operations': 'none', 'max.batch.size': '64', 'max.queue.size': '2048', 'poll.interval.ms': '10',
                  'heartbeat.interval.ms': '1000', 'provide.transaction.metadata': 'false', 'event.converting.failure.handling.mode': 'fail',
                  'errors.tolerance': 'none', 'exactly.once.support': 'requested', 'transaction.boundary': 'connector',
                  'dcg.retention': 'operator-managed', 'dcg.journal': self.library + '/QSQJRN',
                  'schema.history.internal.kafka.bootstrap.servers': 'broker:9092', 'schema.history.internal.kafka.topic': 'bindings-automatic',
                  'producer.override.bootstrap.servers': 'broker:9092', 'admin.override.bootstrap.servers': 'broker:9092',
                  'tombstones.on.delete': 'true', 'topic.naming.strategy': 'io.debezium.schema.SchemaTopicNamingStrategy',
                  'decimal.handling.mode': 'precise', 'binary.handling.mode': 'bytes', 'time.precision.mode': 'adaptive_time_microseconds',
                  'schema.name.adjustment.mode': 'none', 'field.name.adjustment.mode': 'none', 'trim.non.xml.charsequence.field.mode': 'none',
                  'key.converter': 'org.apache.kafka.connect.json.JsonConverter', 'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
                  'key.converter.schemas.enable': 'false', 'value.converter.schemas.enable': 'false'}
        (self.control / 'automatic.owned-fixture').write_text(self.library)
        (self.directory / 'connector.json').write_text(json.dumps(config, indent=2))
        for topic in [f'test.{self.fixture.server_name}.{self.library}.{table}' for table in ['A', 'B', 'M']] + ['events-automatic-control', 'bindings-automatic', '__debezium-heartbeat.test']:
            extra = ['--config', 'cleanup.policy=delete', '--config', 'retention.ms=-1'] if topic == 'bindings-automatic' else []
            self.command('docker', 'exec', self.broker, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'broker:9092',
                         '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1', *extra)
        self.rest(worker, '/connectors', {'name': 'automatic', 'initial_state': 'STOPPED', 'config': config})
        base.wait_for('stopped seed', lambda: self.rest(worker, '/connectors/automatic/status')['connector']['state'] == 'STOPPED')
        seed = {'offset.event_sequence': str(anchor['sequence']), 'offset.time': '0', 'offset.receiver': anchor['receiver'],
                'offset.receiver_library': self.library, 'offset.processed': 'true', 'snapshot_completed': 'true'}
        self.patch_offset(worker, 'automatic', seed)
        self.check('Only ordinary legacy offset seeded', self.checkpoint(self.observer_container, 'automatic', 'seed') == seed, 'control/')
        self.request(worker, 'automatic', 'PUT', '/resume')

    def exercise_automatic(self):
        exp = self.capture
        exp.deadline = None
        writer, transaction = exp.connection(), exp.connection(transaction=True)
        anchor = self.point()
        exp.execute(transaction, 'busy-before-start', f'INSERT INTO {self.library}.U VALUES (90)')
        first = self.start_worker('automatic', 'w1')
        if self.fault == 'pending':
            (self.control / 'automatic.hold-pending').touch()
        elif self.fault in ['barrier', 'migration']:
            (self.control / 'automatic.hold-phase').write_text('handoff-acknowledged' if self.fault == 'barrier' else 'migration-ready')
        self.create_automatic(first, anchor)
        stop = threading.Event()
        progress, errors = [], []

        def write():
            try:
                for value in range(1000, 1600):
                    if stop.wait(.25):
                        break
                    exp.execute(writer, 'legacy-writer', f'INSERT INTO {self.library}.A VALUES ({value})')
                    progress.append(value)
            except BaseException as error:
                errors.append(self.fixture.redact(str(error)))

        actor = threading.Thread(target=write, daemon=True)
        self.writer_actor, self.writer_stop = actor, stop
        actor.start()
        try:
            self.task_marker('busy-*')
            self.check('Busy boundary defers while legacy CDC commits', bool(self.table_records('A', 'busy'))
                       and not list(self.control.glob('automatic-*/handoff-complete.json')), 'control/')
            mutation = exp.connection()
            exp.execute(mutation, 'legacy-key-insert', f'INSERT INTO {self.library}.A VALUES (500)')
            exp.execute(mutation, 'legacy-key-update', f'UPDATE {self.library}.A SET ID=501 WHERE ID=500')
            exp.execute(mutation, 'legacy-key-delete', f'DELETE FROM {self.library}.A WHERE ID=501')
            self.close_session(mutation)

            def key_records():
                rows = [r for r in self.table_records('A', 'legacy-key') if r['key']['ID'] in [500, 501]]
                return rows if len(rows) >= 6 else None

            changed = base.wait_for('legacy key update committed', key_records, 90)
            self.check('Legacy prefix preserves native before-image identity and both update keys',
                       [(r['key']['ID'], r['value']['op'] if r['value'] else None) for r in changed]
                       == [(500, 'c'), (500, 'd'), (500, None), (501, 'c'), (501, 'd'), (501, None)], 'control/')
            exp.execute(transaction, 'rollback')
            self.close_session(transaction)
            if self.fault == 'pending':
                pending = self.task_marker('pending')
                # Make the restart idle deterministically: the old task has
                # acknowledged every completed fixture write before it is killed.
                stop.set()
                actor.join(timeout=25)
                if actor.is_alive() or errors:
                    raise RuntimeError('Legacy writer did not drain before idle restart')

                idle_rows = self.writer_output(progress, 'idle-prefix')
                last_sequence = max(int(r['value']['source']['sequence']) for r in idle_rows)

                def idle_offset():
                    value = self.checkpoint(self.observer_container, 'automatic', 'pending-crash')
                    return value if value and int(value.get('offset.event_sequence', -1)) >= last_sequence else None

                saved_pending = base.wait_for('durable legacy writer boundary before idle restart', idle_offset, 90)
                origin = int(json.loads(pending.read_text())['origin'])
                self.report['idle_restart'] = {'writer_drained': True, 'last_native_row_sequence': last_sequence,
                                               'durable_legacy_sequence': int(saved_pending['offset.event_sequence']), 'pending_origin': origin}
                self.check('Pending history exists before acknowledged handoff with only a durable legacy offset',
                           'offset.event_sequence' in saved_pending and 'offset.version' not in saved_pending
                           and int(saved_pending['offset.event_sequence']) >= origin
                           and not list(self.control.glob('automatic-*/migration-ready.json')), str(pending))
                self.command('docker', 'kill', first)
                (self.control / 'automatic.hold-pending').unlink()
                first = self.start_worker('automatic', 'pending-replacement')
            elif self.fault in ['barrier', 'migration']:
                phase = 'handoff-acknowledged' if self.fault == 'barrier' else 'migration-ready'
                self.task_marker(phase)
                saved_barrier = self.checkpoint(self.observer_container, 'automatic', 'barrier-crash')
                history = self.records(self.observer_container, 'bindings-automatic', 'history-crash')
                self.check('Crash boundary retains the acknowledged legacy barrier before F/W publication',
                           saved_barrier.get('handoff.version') == '1' and 'offset.version' not in saved_barrier
                           and not list(self.control.glob('automatic-*/handoff-complete.json')), 'control/')
                self.command('docker', 'kill', first)
                (self.control / 'automatic.hold-phase').unlink()
                first = self.start_worker('automatic', 'barrier-replacement')
                self.task_marker('handoff-complete')
                after = self.records(self.observer_container, 'bindings-automatic', 'history-recovered')
                self.check('Replacement reconstructs the exact barrier without writing activation history',
                           after == history
                           and not list(self.control.glob('automatic-automatic-barrier-replacement-*/native-boundary.json')), 'control/')
            self.task_marker('handoff-complete')
            if self.fault == 'pending':
                self.check('Replacement uses persisted discovery floor without collecting another boundary',
                           not list(self.control.glob('automatic-automatic-pending-replacement-*/native-boundary.json')), 'control/')
        finally:
            stop.set()
            actor.join(timeout=25)
            exp.report['automatic_writer'] = {'drained': not actor.is_alive(), 'rows': progress, 'errors': errors}
            exp.persist()
        if actor.is_alive() or errors:
            raise RuntimeError('Automatic writer did not drain cleanly')
        self.close_session(writer)
        written = self.writer_output(progress, 'writer-complete')
        self.retain_writer_tail(written[-1])
        # An attached receiver's LAST_SEQUENCE_NUMBER may include entries outside
        # CDC selection. Demand durable acknowledgement of the actual expected
        # row output, never advancement to an arbitrary metadata tail.
        self.row_progress(written[-1], 'writer-complete')
        legacy = self.table_records('A', 'writer-complete')
        concurrent = [r for r in legacy if r['key']['ID'] >= 1000]
        self.check('Task-owned migration emits every concurrent row once without snapshot',
                   [r['value']['after']['ID'] for r in concurrent] == progress and all(r['value']['op'] == 'c' for r in concurrent)
                   and all(r['value'] is None or r['value']['op'] != 'r' for r in legacy), 'control/')
        writer, transaction = exp.connection(), exp.connection(transaction=True)
        exp.execute(transaction, 'open-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(transaction, 'open-reload', f'INSERT INTO {self.library}.B VALUES (200)')
        old, new = self.fixture.rotate()
        exp.execute(writer, 'unrelated-open', f'INSERT INTO {self.library}.A VALUES (200)')
        saved = self.row_progress(self.inserted('A', 200, 'open'), 'open')
        self.check('Unrelated table progresses across rotation while clear stays open',
                   saved['replay_from.receiver'] == old and saved['completed_through.receiver'] == new
                   and self.table_records('A', 'open')[-1]['value']['after']['ID'] == 200
                   and not self.table_records('B', 'open'), 'control/')
        self.command('docker', 'kill', first)
        second = self.start_worker('automatic', 'w2')
        recovered = self.task_marker('recovered', 'automatic-w2')
        self.check('Replacement recovers native F/W without a controller bundle', json.loads(recovered.read_text())['checkpoint'] == saved, str(recovered))
        start = self.point()
        exp.execute(transaction, 'rollback')
        self.progressed(self.terminal(start, transaction[1], 'RB', 'rollback'), 'rollback')
        self.check('Restarted open clear rolls back without output', not self.table_records('B', 'rollback'), 'control/')
        exp.execute(transaction, 'committed-clear', f'TRUNCATE TABLE {self.library}.B REUSE STORAGE IGNORE DELETE TRIGGERS')
        exp.execute(transaction, 'committed-reload', f'INSERT INTO {self.library}.B VALUES (201)')
        start = self.point()
        exp.execute(transaction, 'commit')
        self.progressed(self.terminal(start, transaction[1], 'CM', 'commit'), 'commit')
        self.check('Committed clear precedes reload', [r['value']['op'] for r in self.table_records('B', 'commit')] == ['t', 'c'], 'control/')
        exp.execute(writer, 'key-change', f'UPDATE {self.library}.A SET ID=202 WHERE ID=200')
        self.row_progress(self.inserted('A', 202, 'key-change'), 'key-change')
        changed = self.table_records('A', 'key-change')[-3:]
        self.check('Native primary-key update preserves old-key delete and tombstone before new-key create',
                   len(changed) == 3 and changed[0]['value']['op'] == 'd' and changed[0]['key']['ID'] == 200
                   and changed[1]['value'] is None and changed[1]['key']['ID'] == 200
                   and changed[2]['value']['op'] == 'c' and changed[2]['key']['ID'] == 202, 'control/')
        self.request(second, 'automatic', 'PUT', '/stop')
        self.task_marker('drained', 'automatic-w2')
        before_missing = {table: self.table_records(table, 'before-missing-' + table) for table in ['A', 'B', 'M']}
        history_before = self.records(self.observer_container, 'bindings-automatic', 'history-before-missing')
        self.patch_offset(second, 'automatic', None)
        self.check('Offset deletion is independently visible', self.checkpoint(self.observer_container, 'automatic', 'missing') is None, 'control/')
        self.request(second, 'automatic', 'PUT', '/resume')
        base.wait_for('missing-offset rejection', lambda: any(t['state'] == 'FAILED'
                     for t in self.rest(second, '/connectors/automatic/status')['tasks']), 45)
        self.check('Missing offset rejects without output or binding-history changes',
                   all(self.table_records(table, 'after-missing-' + table) == records for table, records in before_missing.items())
                   and self.records(self.observer_container, 'bindings-automatic', 'history-after-missing') == history_before, 'control/')
        self.fixture.verify_rows({'A': sorted([1, 2, 3, *progress, 202]), 'B': [201], 'M': [1, 2, 3], 'U': [1, 2, 3]})
        self.report['status'] = 'passed'
        self.save()

    def cleanup(self):
        if self.writer_stop:
            self.writer_stop.set()
        if self.writer_actor:
            self.writer_actor.join(timeout=25)
        super().cleanup()
        if self.writer_actor and self.writer_actor.is_alive():
            self.report.update(status='failed', native_worker_cleanup={'status': 'failed', 'errors': ['Fixture writer did not drain']})
            self.save()
            raise RuntimeError('Fixture writer still running; fixture deletion blocked')

    def retain_failure_evidence(self):
        # Observe committed broker state independently of the failed source task.
        # An observation failure is recorded and must not prevent cleanup.
        if not hasattr(self, 'observer_container') or not (self.control / 'automatic.owned-fixture').exists():
            return
        errors = []
        for label, action in [
                ('offset', lambda: self.checkpoint(self.observer_container, 'automatic', 'failure')),
                *[(table, lambda table=table: self.table_records(table, 'failure-' + table)) for table in ['A', 'B', 'M']],
                ('control', lambda: self.records(self.observer_container, 'events-automatic-control', 'failure-control')),
                ('history', lambda: self.records(self.observer_container, 'bindings-automatic', 'failure-history'))]:
            try:
                action()
            except Exception as error:
                errors.append({'observation': label, 'error': self.fixture.redact(str(error))})
        self.report['independent_failure_evidence'] = {'directory': 'control/', 'errors': errors}
        self.save()


def main():
    if len(sys.argv) not in [3, 4]:
        raise SystemExit('Usage: dcg-automatic-task-probe.py NEW_DIRECTORY DCNCdddd [none|pending|barrier|migration]; isolated IBM i writes and local Kafka')
    import pyodbc
    pyodbc.pooling = False
    root = Path(sys.argv[1]).resolve()
    root.mkdir(exist_ok=False)
    capture = live.prospective.census.Experiment(root / 'native', sys.argv[2], 1)
    experiment = None
    try:
        capture.fixture.setup()
        experiment = Experiment(root / 'connect', capture, sys.argv[3] if len(sys.argv) == 4 else 'none')
        experiment.stage()
        experiment.start_broker()
        experiment.observer()
        experiment.exercise_automatic()
        capture.report.update(status='passed', source_connected=True, publication='local Kafka under explicit test admission')
    except BaseException as error:
        (root / 'failure.txt').write_text(capture.fixture.redact(traceback.format_exc()))
        capture.report.update(status='failed', error=capture.fixture.redact(type(error).__name__ + ': ' + str(error)))
        if experiment:
            experiment.report.update(status='failed', error=capture.fixture.redact(str(error)))
            experiment.save()
            experiment.retain_failure_evidence()
    finally:
        live.cleanup_owned(experiment, capture)
    if not experiment or experiment.report.get('status') != 'passed' or capture.report.get('fixture_cleanup', {}).get('status') != 'passed':
        raise SystemExit('Automatic task qualification failed; evidence and cleanup retained')
    print(experiment.directory / 'report.json', flush=True)


if __name__ == '__main__':
    main()
