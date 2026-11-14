#!/usr/bin/env python3
# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Bounded packaged-connector comparison; isolated native fixture and local Kafka.

The distribution is explicit so the identical runner can load the pre-feature
connector or the candidate. Nothing measures source throughput from task start.
Warm-up is excluded and each timed burst validates every expected key/value.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import threading
import time
import traceback
import zipfile

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('public_performance', Path(__file__).with_name('dcg-public-online-probe.py'))
public = importlib.util.module_from_spec(spec)
spec.loader.exec_module(public)
base, live = public.base, public.live


def percentile(values, fraction):
    return sorted(values)[max(0, min(len(values) - 1, int(len(values) * fraction + .999999) - 1))]


class Experiment(public.Experiment):
    timed_rounds = 7

    def __init__(self, directory, capture, args):
        super().__init__(directory, capture)
        self.args = args
        self.events = []
        self.event_position = 0
        self.samples = []
        self.measurements = []
        self.sample_stop = threading.Event()
        self.sampler = None

    def stage(self):
        super().stage()
        # Only the selected distribution may enter the worker's plugin path.
        for path in (self.plugin / 'ibmi').iterdir():
            path.unlink()
        with zipfile.ZipFile(self.args.distribution) as archive:
            for item in archive.infolist():
                if item.filename.endswith('.jar'):
                    (self.plugin / 'ibmi' / Path(item.filename).name).write_bytes(archive.read(item))
        connector = self.plugin / 'ibmi/debezium-connector-ibmi-3.8.0-SNAPSHOT.jar'
        with zipfile.ZipFile(connector) as archive:
            if any(name.endswith('Test.class') or 'ProbeConnector' in name for name in archive.namelist()):
                raise RuntimeError('Selected worker distribution contains test classes')
        classes = ROOT / 'debezium-connector-ibmi/target/test-classes'
        with zipfile.ZipFile(self.audit / 'performance-audit.jar', 'w') as archive:
            for path in classes.rglob('As400PerformanceProbeAudit*.class'):
                archive.write(path, str(path.relative_to(classes)))
        (self.audit / 'performance.jfc').write_text('''<?xml version="1.0" encoding="UTF-8"?>
<configuration version="2.0" label="Bounded connector comparison">
<event name="jdk.SocketRead"><setting name="enabled">true</setting><setting name="threshold">0 ms</setting><setting name="stackTrace">false</setting></event>
<event name="jdk.SocketWrite"><setting name="enabled">true</setting><setting name="threshold">0 ms</setting><setting name="stackTrace">false</setting></event>
<event name="jdk.GarbageCollection"><setting name="enabled">true</setting><setting name="threshold">0 ms</setting></event>
<event name="jdk.GCHeapSummary"><setting name="enabled">true</setting></event>
<event name="jdk.ExecutionSample"><setting name="enabled">true</setting><setting name="period">20 ms</setting></event>
</configuration>
''')
        self.report.update(scope='Identical bounded native workload and continuous read_committed observer; ordinary packaged worker',
                           mode=self.args.mode, version=self.args.label, rows_per_burst=self.args.rows,
                           buffer_storage=self.args.buffer_storage,
                           failure_handling=self.args.failure_handling,
                           legacy_additional_delay_ms=getattr(self.args, 'journal_additional_delay', None),
                           inherited_worker_defaults=self.args.worker_defaults,
                           migration_enabled=self.args.mode == 'transactional', backlog_rows=self.args.backlog_rows,
                           warmup_rounds=2, measured_rounds=max(0, self.timed_rounds-2), samples=self.samples, measurements=self.measurements,
                           distribution_sha256=hashlib.sha256(self.args.distribution.read_bytes()).hexdigest(),
                           plugin_sha256={str(p.relative_to(self.plugin)): hashlib.sha256(p.read_bytes()).hexdigest()
                                          for p in (self.plugin / 'ibmi').iterdir()},
                           initialization='Real legacy snapshot/streaming; online handoff only for transactional mode',
                           timing='Host submit to read_committed observation; source/server clocks not subtracted',
                           jfr='Socket operations are transport observations, not one-to-one SQL/RPC counts')
        for relative in ['tools/dcg-performance-probe.py',
                         'debezium-connector-ibmi/src/test/java/io/debezium/connector/db2as400/As400PerformanceProbeAudit.java']:
            self.report['harness_sha256'][relative] = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        self.save()

    def worker_java_options(self):
        return (super().worker_java_options() + ' -XX:ActiveProcessorCount=2 '
                '-XX:StartFlightRecording=filename=/control/performance.jfr,settings=/audit/performance.jfc,dumponexit=true,maxsize=128m')

    def observer_arguments(self):
        return []

    def extended_workload(self, worker, session):
        pass

    def configuration(self, enabled):
        config = super().configuration(enabled)
        config['event.converting.failure.handling.mode'] = self.args.failure_handling
        delay = getattr(self.args, 'journal_additional_delay', None)
        if delay is not None:
            if self.args.mode != 'legacy':
                raise ValueError('Legacy delay control applies only to the legacy reader')
            config['journal.additional.delay'] = str(delay)
        if self.args.worker_defaults:
            for key in ['key.converter', 'value.converter', 'key.converter.schemas.enable', 'value.converter.schemas.enable',
                        'tasks.max', 'errors.tolerance']:
                config.pop(key, None)
        if enabled:
            config['truncate.buffer.storage'] = self.args.buffer_storage
        if not enabled:
            config.pop('truncate.transactional.enabled', None)
            for key in list(config):
                if key.startswith('dcg.'):
                    config.pop(key)
        return config

    def new_events(self):
        path = self.control / 'performance-events.jsonl'
        if path.exists():
            with path.open() as stream:
                stream.seek(self.event_position)
                while True:
                    start = stream.tell()
                    line = stream.readline()
                    if not line or not line.endswith('\n'):
                        self.event_position = start
                        break
                    self.events.append(json.loads(line))
        return self.events

    def matching(self, table, first, count, operation):
        return [r for r in self.new_events() if r['topic'].endswith('.' + table) and r['value'] is not None
                and r['value']['op'] == operation and first <= r['key']['ID'] < first + count]

    def monitor(self, pid):
        while not self.sample_stop.wait(.2):
            try:
                status = Path(f'/proc/{pid}/status').read_text()
                fields = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
                rss = int(next(line.split()[1] for line in status.splitlines() if line.startswith('VmRSS:')))
                self.samples.append({'at_ms':time.time_ns() / 1e6, 'rss_kib':rss,
                                     'cpu_seconds':(int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')})
            except FileNotFoundError:
                return

    def write_rows(self, session, table, first, count, operation):
        if operation == 'u':
            sql = f"UPDATE {self.library}.M SET S=-123,V='XYZ' WHERE ID>={first} AND ID<{first+count}"
        else:
            values = ','.join('(' + str(i) + (",12,123.45,1234.567,'ABC','Ä![]',BX'00010203',BX'00FF',9007199254740993,1.25,-2.5" if table == 'M' else '') + ')'
                              for i in range(first, first+count))
            sql = f'INSERT INTO {self.library}.{table} VALUES ' + values
        cursor = session[0].cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def await_rows(self, table, first, count, operation):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            rows = self.matching(table, first, count, operation)
            if len(rows) >= count:
                break
            if self.observer_process.poll() is not None:
                raise RuntimeError('Continuous observer exited before burst completion')
            time.sleep(.025)
        else:
            raise RuntimeError('CDC burst did not finish within 180 seconds')
        if len(rows) != count or sorted(r['key']['ID'] for r in rows) != list(range(first, first+count)):
            raise AssertionError('Missing, duplicate or foreign CDC key')
        for row in rows:
            after = row['value']['after']
            if after['ID'] != row['key']['ID'] or (operation == 'u' and (after['S'] != -123 or after['V'] != 'XYZ')):
                raise AssertionError('CDC value differs from native workload')
            if table == 'M':
                expected = public.packaged.continuous.ContinuousExperiment.expected_mixed(after['ID'])
                if operation == 'u':
                    before = row['value']['before']
                    if before is None or public.packaged.continuous.ContinuousExperiment.decode_mixed(before) != expected:
                        raise AssertionError('Mixed update before-image differs')
                    expected.update(S=-123, V='XYZ')
                decoded = public.packaged.continuous.ContinuousExperiment.decode_mixed(after)
                if decoded != expected:
                    raise AssertionError('Mixed native values differ')
        return rows

    def record(self, rows, started, source_finished, scenario, round_number, **extra):
        elapsed = max(r['observed_ms'] for r in rows) - started
        latencies = [r['observed_ms']-started for r in rows]
        if min(latencies) < 0 or elapsed <= 0:
            raise AssertionError('Observer clock or record selection is invalid')
        record = {'scenario':scenario,'round':round_number,'warmup':round_number<2,'rows':len(rows),
                  'started_ms':started,'source_finished_ms':source_finished,'finished_ms':max(r['observed_ms'] for r in rows),
                  'source_sql_ms':source_finished-started,'elapsed_ms':elapsed,'rows_per_second':len(rows)*1000/elapsed,
                  'p50_ms':statistics.median(latencies),'p95_ms':percentile(latencies,.95),'p99_ms':percentile(latencies,.99), **extra}
        self.measurements.append(record); self.save()
        print(json.dumps(record), flush=True)

    def burst(self, session, table, first, operation, round_number):
        started = time.time_ns() / 1e6
        self.write_rows(session, table, first, self.args.rows, operation)
        source_finished = time.time_ns() / 1e6
        rows = self.await_rows(table, first, self.args.rows, operation)
        self.record(rows, started, source_finished, table+'-'+operation, round_number)

    def stop_connector(self, worker):
        self.request(worker, 'automatic', 'PUT', '/stop')
        def stopped():
            status = self.rest(worker, '/connectors/automatic/status')
            return status['connector']['state'] == 'STOPPED' and not status['tasks']
        base.wait_for('connector and tasks stopped', stopped, 60)

    def backlog(self, worker, session, round_number):
        self.stop_connector(worker)
        before = len(self.new_events())
        count = self.args.backlog_rows // 2
        first = 100000 + round_number * 10000
        loading_started = time.monotonic()
        for table in ['A', 'M']:
            for offset in range(0, count, self.args.rows):
                self.write_rows(session, table, first+offset, min(self.args.rows, count-offset), 'c')
        loading_seconds = time.monotonic() - loading_started
        if len(self.new_events()) != before:
            raise AssertionError('Stopped connector published while loading backlog')
        started = time.time_ns() / 1e6
        self.request(worker, 'automatic', 'PUT', '/resume')
        rows = self.await_rows('A', first, count, 'c') + self.await_rows('M', first, count, 'c')
        self.record(rows, started, started, 'backlog-resume', round_number,
                    warmup=False, preload_seconds=loading_seconds,
                    first_visible_ms=min(r['observed_ms'] for r in rows)-started,
                    visible_span_ms=max(r['observed_ms'] for r in rows)-min(r['observed_ms'] for r in rows),
                    timing='Resume request to last visible record; preload excluded, restart validation included')

    def exercise(self):
        worker = self.start_worker('automatic','w1')
        self.setup_connector(worker)
        if self.args.mode == 'transactional':
            self.request(worker,'automatic','PUT','/config',self.configuration(True))
        self.request(worker,'automatic','PUT','/resume')
        if self.args.mode == 'transactional':
            self.lifecycle(worker,'handoff-complete')
        else:
            base.wait_for('ordinary task resumed', lambda: any(t['state']=='RUNNING' for t in self.rest(worker,'/connectors/automatic/status')['tasks']),90)
        top = self.command('docker','top',worker,'-eo','pid,args')
        pids = [int(line.split()[0]) for line in top.splitlines() if 'org.apache.kafka.connect.cli.ConnectDistributed' in line]
        if len(pids)!=1:
            raise RuntimeError('Exactly one owned Connect JVM required')
        pid = pids[0]
        self.sampler = threading.Thread(target=self.monitor,args=(pid,),daemon=True); self.sampler.start()
        self.observer_process = subprocess.Popen(['docker','exec',self.observer_container,'java','-cp','/audit/*:/opt/kafka/libs/*',
                        'io.debezium.connector.db2as400.As400PerformanceProbeAudit',self.fixture.server_name,self.library,*self.observer_arguments()],
                        stdout=(self.directory/'observer.log').open('w'),stderr=subprocess.STDOUT)
        self.children.append(self.observer_process)
        base.wait_for('continuous observer ready',lambda:(self.control/'performance-ready').exists(),30)
        session=self.capture.connection()
        for round_number in range(self.timed_rounds):
            first=10000+round_number*1000
            self.burst(session,'A',first,'c',round_number)
            self.burst(session,'M',first,'c',round_number)
            self.burst(session,'M',first,'u',round_number)
        if self.args.backlog_rows:
            for round_number in range(2):
                self.backlog(worker, session, round_number)
        self.check('Every timed burst preserves exact keys and mixed values',
                   len(self.measurements)==self.timed_rounds*3+(2 if self.args.backlog_rows else 0),'control/performance-events.jsonl')
        self.extended_workload(worker, session)
        self.close_session(session)
        self.stop_connector(worker)
        (self.control/'performance-stop').touch()
        if self.observer_process.wait(timeout=15)!=0:
            raise RuntimeError('Continuous observer did not exit cleanly')
        self.new_events()
        self.check('Observer contains exactly the timed output without late duplicates',
                   len(self.events)==sum(m['rows'] for m in self.measurements)+getattr(self,'additional_events',0),'control/performance-events.jsonl')
        self.check('Worker CPU and resident memory were sampled',len(self.samples)>=3,'report.json')
        self.sample_stop.set(); self.sampler.join(5)
        # PID is read from this owned container again before signalling its JVM.
        current=self.command('docker','top',worker,'-eo','pid,args')
        if not any(line.split()[0]==str(pid) and 'org.apache.kafka.connect.cli.ConnectDistributed' in line for line in current.splitlines()[1:]):
            raise RuntimeError('Owned JVM identity changed before recording drain')
        if self.args.heap_histogram:
            # Deliberately outside every timing interval and after task shutdown.
            # jcmd performs a full GC; this is retained-heap diagnosis, not RSS timing.
            try:
                result = subprocess.run(['jcmd', str(pid), 'GC.class_histogram'], text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
                diagnostic = result.stdout
                status = 'passed' if result.returncode == 0 else 'unavailable'
            except (OSError, subprocess.TimeoutExpired) as error:
                diagnostic, status = str(error), 'unavailable'
            (self.directory / 'stopped-heap-histogram.txt').write_text(self.fixture.redact(diagnostic))
            self.report['stopped_heap_diagnostic'] = {'status': status, 'outside_timed_intervals': True, 'full_gc_requested': True}
        os.kill(pid,signal.SIGTERM)
        base.wait_for('flight recording drained',lambda:(self.control/'performance.jfr').exists() and all(p.poll() is not None for p in self.children),45)
        self.report['status']='passed'; self.save()


def main(experiment_type=Experiment, configure_parser=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path); parser.add_argument('library')
    parser.add_argument('--distribution',type=Path,required=True)
    parser.add_argument('--label',required=True); parser.add_argument('--mode',choices=['legacy','transactional'],default='legacy')
    parser.add_argument('--rows',type=int,default=250)
    parser.add_argument('--buffer-storage',choices=['memory','disk'],default='memory')
    parser.add_argument('--failure-handling', choices=['fail','warn','skip'], default='fail',
                        help='Use the same ordinary row-error policy for baseline and candidate measurements')
    parser.add_argument('--worker-defaults', action='store_true', help='Inherit worker converters and standard task/error defaults')
    parser.add_argument('--backlog-rows',type=int,default=0, help='Optional even backlog, split between A and M; two restart trials')
    parser.add_argument('--journal-additional-delay', type=int, choices=[0, 5000], default=None,
                        help='Isolated legacy-reader timing control only; not a production tuning recommendation')
    parser.add_argument('--heap-histogram',action='store_true', help='After task stop and all measurements, request a full-GC histogram using host JDK jcmd')
    if configure_parser:
        configure_parser(parser)
    args=parser.parse_args()
    if not 50<=args.rows<=500 or not args.distribution.is_file():
        parser.error('Use an existing assembly ZIP and 50..500 rows per burst')
    if args.backlog_rows not in (0, 2000, 5000):
        parser.error('Backlog must be 0, 2000 or 5000 records')
    if args.mode == 'legacy' and args.buffer_storage != 'memory':
        parser.error('Disk transaction storage applies only to transactional mode')
    if args.journal_additional_delay is not None and args.mode != 'legacy':
        parser.error('Legacy delay control applies only to the legacy reader')
    if hasattr(args, "profile_validator"):
        try:
            args.profile_validator(args)
        except ValueError as error:
            parser.error(str(error))
    import pyodbc
    pyodbc.pooling=False
    args.directory=args.directory.resolve(); args.directory.mkdir(exist_ok=False)
    capture=live.prospective.census.Experiment(args.directory/'native',args.library,1)
    capture.fixture.__class__=public.packaged.continuous.MixedFixture
    experiment=None
    try:
        capture.fixture.setup()
        experiment=experiment_type(args.directory/'connect',capture,args)
        experiment.stage(); experiment.start_broker(); experiment.observer(); experiment.exercise()
        capture.report.update(status='passed',source_connected=True)
    except BaseException as error:
        (args.directory/'failure.txt').write_text(capture.fixture.redact(traceback.format_exc()))
        capture.report.update(status='failed',error=capture.fixture.redact(str(error)))
        if experiment:
            experiment.report.update(status='failed',error=capture.fixture.redact(str(error)))
            experiment.save(); experiment.retain_failure_evidence()
    finally:
        if experiment:
            experiment.sample_stop.set()
            if experiment.sampler: experiment.sampler.join(5)
        live.cleanup_owned(experiment,capture)
    if not experiment or experiment.report.get('status')!='passed' or capture.report.get('fixture_cleanup',{}).get('status')!='passed':
        raise SystemExit('Performance run failed; evidence and cleanup retained')
    print(experiment.directory/'report.json',flush=True)


if __name__=='__main__':
    main()
