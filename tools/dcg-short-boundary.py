# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
"""Offline, test-only lock pulse generator and N-marker consistency checks.

There is deliberately no connection, native executor, retry, or startup admission
here. Scope completeness, native handler semantics and independent holder drain
must be qualified separately. A matching marker is never a migration certificate.
"""
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re


PROFILE = 'offlock-metadata-marker-candidate-v1'
RESULT_TABLE = 'SESSION.DCGPULSE'
PREPARE_SQL = (
    'DECLARE GLOBAL TEMPORARY TABLE SESSION.DCGPULSE '
    '(ATTEMPT CHAR(32), SCOPE_DIGEST CHAR(64), ACQUIRED TIMESTAMP(6), RELEASED TIMESTAMP(6)) '
    'ON COMMIT PRESERVE ROWS NOT LOGGED'
)


def exact(value, pattern, description):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError(description)
    return value


def sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class Pulse:
    """Own a structurally checked scope; its hash does not prove native completeness.

    Targets are (file, actual member) pairs from the native scope validator, all
    in one disposable library. evidence_sha256 binds the off-lock evidence bundle.
    A fresh, dedicated holder with no prior target allocations is a prerequisite.
    """
    library: str
    targets: tuple
    nonce: str
    evidence_sha256: str

    def __post_init__(self):
        exact(self.library, r'DCNC[0-9]{4}', 'Exact isolated library required')
        exact(self.nonce, r'[A-F0-9]{32}', '32-character attempt nonce required')
        exact(self.evidence_sha256, r'[a-f0-9]{64}', 'Off-lock evidence digest required')
        if not isinstance(self.targets, (list, tuple)) or not 1 <= len(self.targets) <= 50:
            raise ValueError('One bounded, complete scope of 1..50 files required')
        owned = []
        for target in self.targets:
            if not isinstance(target, (tuple, list)) or len(target) != 2:
                raise ValueError('Exact file/member pair required')
            file, member = target
            exact(file, r'[A-Z][A-Z0-9_]{0,9}', 'Exact file name required')
            exact(member, r'[A-Z0-9_#$@]{1,10}', 'Exact member name required')
            owned.append((file, member))
        if owned != sorted(owned) or len({file for file, _ in owned}) != len(owned):
            raise ValueError('Distinct ordered single-member files required')
        object.__setattr__(self, 'targets', tuple(owned))

    @property
    def scope_digest(self):
        canonical = json.dumps({'journal': [self.library, 'QSQJRN'], 'targets': self.targets,
                                'evidence_sha256': self.evidence_sha256}, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(canonical.encode('ascii')).hexdigest().upper()

    @property
    def payload(self):
        # Letters, digits and spaces only. This first local fixture profile uses
        # CCSID 37 explicitly; native byte/CCSID fidelity remains a qualification gate.
        return f'DCGB1 {self.nonce} N {self.scope_digest}'

    @property
    def objects(self):
        return ' '.join(f'({self.library}/{file} *FILE *EXCLRD {member})' for file, member in self.targets)

    @property
    def allocate(self):
        return f'ALCOBJ OBJ({self.objects}) WAIT(1) CONFLICT(*RQSRLS) SCOPE(*JOB)'

    @property
    def release(self):
        return f'DLCOBJ OBJ({self.objects}) SCOPE(*JOB)'

    @property
    def marker(self):
        return (f'SNDJRNE JRN({self.library}/QSQJRN) TYPE(DN) ENTDTA({sql_literal(self.payload)}) '
                'FILE(*NONE) FORCE(*NO) OVRSTATE(*NONE) OVRRMTFTR(*NONE)')

    def statement(self):
        """One server statement with at most three same-job release attempts.

        The caller must independently exclude pre-existing matching allocations.
        Mark attempted allocation before ALCOBJ so an uncertain partial return
        still reaches cleanup. Any warning/error permanently rejects the attempt,
        even if a later release succeeds. No metadata runs while locks are held.
        """
        handlers = lambda flag: [f'DECLARE EXIT HANDLER FOR {condition} SET {flag}=1;'
                                 for condition in ('SQLEXCEPTION', 'SQLWARNING', 'NOT FOUND')]
        lines = ['BEGIN NOT ATOMIC', 'DECLARE HELD SMALLINT DEFAULT 0;',
                 'DECLARE FAILED SMALLINT DEFAULT 0;', 'DECLARE RELEASE_FAILED SMALLINT DEFAULT 0;',
                 'DECLARE ACQUIRED TIMESTAMP(6);', 'DECLARE RELEASED TIMESTAMP(6);', 'BEGIN']
        lines += handlers('FAILED')
        lines += ['SET HELD=1;', f'CALL QSYS2.QCMDEXC({sql_literal(self.allocate)});',
                  'SET ACQUIRED=CURRENT_TIMESTAMP;', f'CALL QSYS2.QCMDEXC({sql_literal(self.marker)});', 'END;']
        for _ in range(3):
            lines += ['IF HELD=1 THEN', 'SET RELEASE_FAILED=0;', 'BEGIN']
            lines += handlers('RELEASE_FAILED')
            lines += [f'CALL QSYS2.QCMDEXC({sql_literal(self.release)});', 'SET HELD=0;',
                      'SET RELEASED=CURRENT_TIMESTAMP;', 'END;', 'IF RELEASE_FAILED=1 THEN',
                      'SET FAILED=1;', 'END IF;', 'END IF;']
        lines += ['IF FAILED=0 AND HELD=0 THEN', 'BEGIN']
        lines += handlers('FAILED')
        lines += [f'INSERT INTO {RESULT_TABLE} VALUES({sql_literal(self.nonce)}, '
                  f'{sql_literal(self.scope_digest)}, ACQUIRED, RELEASED);', 'END;', 'END IF;',
                  'IF HELD=1 THEN',
                  "SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT='Pulse release failed; independent drain required';", 'END IF;',
                  'IF FAILED=1 THEN',
                  "SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Pulse failed; independent drain required';", 'END IF;', 'END']
        return '\n'.join(lines)

    @property
    def program_sha256(self):
        return hashlib.sha256(self.statement().encode('ascii')).hexdigest()


def validate_marker(pulse, records, context, completion, result_rows):
    """Check one N marker in a bounded, caller-supplied decoded U-entry slice.

    This is an offline fixture contract, not a native evidence collector. The
    caller must later prove the context, bounds and complete audit independently.
    No filtering out foreign records or trimming/converting their payload occurs.
    Copies only owned bytes/scalars into an explicitly unqualified result.
    """
    expected_attempt = {'nonce': pulse.nonce, 'scope_digest': pulse.scope_digest,
                        'program_sha256': pulse.program_sha256}
    if any(completion.get(key) != value for key, value in expected_attempt.items()):
        raise ValueError('Foreign or stale pulse completion')
    if (completion.get('response') != 'success' or completion.get('warnings') != []
            or completion.get('holder_drained') is not True or completion.get('locks_absent') is not True):
        raise ValueError('Failed, lost, warned or undrained pulse')
    if not isinstance(result_rows, (list, tuple)) or len(result_rows) != 1:
        raise ValueError('Exactly one post-release result required')
    row = result_rows[0]
    if row.get('ATTEMPT') != pulse.nonce or row.get('SCOPE_DIGEST') != pulse.scope_digest:
        raise ValueError('Foreign post-release result')
    # Use normalized, timezone-free server timestamp strings supplied by the
    # fixture adapter; comparing malformed/mixed timestamp representations is unsafe.
    try:
        times = [datetime.strptime(row[key], '%Y-%m-%d %H:%M:%S.%f') for key in ('ACQUIRED', 'RELEASED')]
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError('Missing or malformed server timing') from error
    if times[1] < times[0]:
        raise ValueError('Release precedes acquisition')

    for key in ('journal_identity', 'receiver_identity'):
        if not isinstance(context.get(key), bytes) or not context[key] or len(context[key]) > 4096:
            raise ValueError('Owned native identity required')
    exact(context.get('receiver'), r'[A-Z][A-Z0-9_]{0,9}', 'Exact receiver name required')
    exact(context.get('job'), r'[0-9]{6}/[A-Z0-9_#$@]{1,10}/[A-Z0-9_#$@]{1,10}', 'Exact holder job required')
    if not isinstance(context.get('thread'), bytes) or len(context['thread']) != 8 or not any(context['thread']):
        raise ValueError('Actual owned nonzero thread identity required')
    low, high = context.get('lower_sequence'), context.get('upper_sequence')
    if type(low) is not int or type(high) is not int or not 0 < low < high:
        raise ValueError('Bounded same-receiver lookup required')
    if not isinstance(records, (list, tuple)) or len(records) != 1:
        raise ValueError('Exactly one marker required; absence is not success')
    record = records[0]
    expected = {'journal_library': pulse.library, 'journal': 'QSQJRN', 'receiver_library': pulse.library,
                'receiver': context['receiver'], 'journal_identity': context['journal_identity'],
                'receiver_identity': context['receiver_identity'], 'job': context['job'], 'thread': context['thread'],
                'code': 'U', 'type': 'DN', 'file': None, 'member': None}
    if any(record.get(key) != value for key, value in expected.items()) or any(key not in record for key in expected):
        raise ValueError('Foreign marker identity, job, receiver or record kind')
    for key, value in (('commit_cycle', 0), ('ccsid', 37)):
        if type(record.get(key)) is not int or record[key] != value:
            raise ValueError('Marker must be independent and use the qualified fixture CCSID')
    if (record.get('truncated') is not False or record.get('pointer_backed') is not False
            or record.get('warnings') != []):
        raise ValueError('Incomplete, pointer-backed or warned marker')
    sequence = record.get('sequence')
    if type(sequence) is not int or not low < sequence < high:
        raise ValueError('Marker outside the bounded receiver interval')
    raw = record.get('payload')
    if not isinstance(raw, (bytes, bytearray, memoryview)) or not 0 < len(raw) <= 512:
        raise ValueError('Bounded owned marker bytes required')
    raw = bytes(raw)
    if type(record.get('data_length')) is not int or record['data_length'] != len(raw):
        raise ValueError('Incomplete marker payload length')
    if raw != pulse.payload.encode('cp037'):
        raise ValueError('Marker version, attempt, phase, scope or bytes differ')
    return {'profile': PROFILE, 'status': 'marker-consistent-unqualified', 'startup_proof': False,
            'maintenance_visibility_proven': False, **expected_attempt,
            'marker': {'receiver_library': pulse.library, 'receiver': context['receiver'],
                       'sequence': sequence, 'payload_hex': raw.hex()}}


if __name__ == '__main__':
    # Offline input/output only. Never execute the generated SQL here.
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('request', type=Path, help='Local JSON: library, targets, nonce, evidence_sha256')
    args = parser.parse_args()
    request = json.loads(args.request.read_text())
    program = Pulse(**request)
    print(json.dumps({'profile': PROFILE, 'status': 'generated-only', 'startup_proof': False,
                      'maintenance_visibility_proven': False, 'source_connected': False,
                      'scope_digest': program.scope_digest, 'program_sha256': program.program_sha256,
                      'prepare_sql': PREPARE_SQL, 'pulse_sql': program.statement(),
                      'marker_payload_hex_ccsid37': program.payload.encode('cp037').hex()}, indent=2))
