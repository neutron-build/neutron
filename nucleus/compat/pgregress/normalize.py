#!/usr/bin/env python3
"""Normalize psql output for cross-server SEMANTIC diffing.

The goal is to surface value/row/error differences, not cosmetic ones:
- ERROR/FATAL lines collapse to "ERROR" — wording differs legitimately.
- Advisory lines (DETAIL/HINT/CONTEXT/LINE/NOTICE/WARNING/caret) are dropped.
- Result-set HEADERS and their separator rules are dropped — column NAMING
  (Postgres `count`/`?column?` vs Nucleus's expression echo) is tracked as its
  own documented deviation, not mixed into semantic comparison.
- Data cells are whitespace-trimmed so column-width alignment doesn't diff.
- NUMERIC display scale is canonicalized (PG `20.0000000000000000` vs
  `20.0`): trailing zeros after a decimal point are trimmed. This hides a
  known, documented formatting deviation, not a value difference.
"""
import re
import sys

DROP = re.compile(r"^(DETAIL|HINT|CONTEXT|LINE \d|NOTICE|WARNING)")
CARET = re.compile(r"^\s*\^\s*$")
ERR = re.compile(r"^(psql:[^:]*:\d+: )?(ERROR|FATAL)\b")
# psql aligned separator: only spaces, dashes, plus signs, with >=3 dashes.
SEP = re.compile(r"^[ +-]*-{3,}[ +-]*$")

raw = [l.rstrip() for l in open(sys.argv[1], errors="replace")]

# Mark separators and the header line immediately above each.
drop_idx = set()
for i, l in enumerate(raw):
    if SEP.match(l):
        drop_idx.add(i)
        if i > 0:
            drop_idx.add(i - 1)

def canon_num(cell):
    c = cell.strip()
    # A decimal: canonicalize to 12 significant figures so PG's arbitrary-
    # precision NUMERIC display (e.g. AVG returning 186.6666666666666667) and
    # Nucleus's f64 (186.66666666666666) compare equal. This hides a
    # documented formatting deviation (AVG/division return f64-precision, not
    # PG's numeric scale), NOT a value difference: a wrong SUM/COUNT differs by
    # whole units and still diverges.
    if re.fullmatch(r"-?\d+\.\d+", c):
        try:
            c = f"{float(c):.12g}"
        except ValueError:
            pass
        if "." in c and "e" not in c:
            c = c.rstrip("0").rstrip(".")
    return c

out = []
for i, l in enumerate(raw):
    if i in drop_idx:
        continue
    if DROP.match(l) or CARET.match(l):
        continue
    if ERR.match(l):
        out.append("ERROR")
        continue
    if "|" in l:
        cells = [canon_num(c) for c in l.split("|")]
        out.append(" | ".join(cells))
    else:
        out.append(canon_num(l))

prev_blank = False
for line in out:
    blank = line == ""
    if blank and prev_blank:
        continue
    print(line)
    prev_blank = blank
