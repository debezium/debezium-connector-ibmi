/* Copyright Debezium Authors. Licensed under the Apache License, */
/* Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0). */
PGM PARM(&P &S)
DCL VAR(&P) TYPE(*CHAR) LEN(537)
DCL VAR(&S) TYPE(*CHAR) LEN(8708)
DCL VAR(&N) TYPE(*INT) LEN(4)
DCL VAR(&AT) TYPE(*INT) LEN(4)
IF COND(%BIN(&P 1 4) *NE 537) THEN(GOTO CMDLBL(BAD))
/* Keep only the target and its historical SQL names. Other */
/* references (keys, triggers, journals) cannot establish selection. */
IF COND(%BIN(&P 13 4) *NE 3) THEN(RETURN)
IF COND((%BIN(&P 533 4) *NE 1) *AND +
 (%BIN(&P 533 4) *NE 3) *AND +
 (%BIN(&P 533 4) *NE 4)) THEN(RETURN)
CHGVAR VAR(&N) VALUE(%BIN(&S 1 4))
IF COND((&N *LT 0) *OR (&N *GE 16)) THEN(GOTO CMDLBL(BAD))
CHGVAR VAR(&AT) VALUE(5 + &N * 544)
CHGVAR VAR(%SST(&S &AT 537)) VALUE(&P)
CHGVAR VAR(%BIN(&S 1 4)) VALUE(&N + 1)
RETURN
BAD: CHGVAR VAR(%BIN(&S 1 4)) VALUE(-1)
ENDPGM
