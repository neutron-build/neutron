//! Card A-6b's tests of the walk, the segments and the appender on MemEnv
//! and the hook environment (Work 11): the §15.1 forms this card owns
//! (`t_stale_boot_record`, `t_wal_splice`, the walk's stop at the first
//! wrong `lsn`, and the forms S-STORE 7.17 names for A-6b:
//! `t_wal_short_read`, `t_wal_read_error`, `t_wal_malformed`), the decision
//! table, the ends, "writes nothing", the names, the writer's tail in every
//! room, the appender's errors and the order of every write-class call.
//!
//! Every expected value is computed here from the spec's sentences
//! (`oracle_place`, `assemble`, the plan placers of `wal_common`), never
//! read from the code under test. Where the card (written against S-STORE
//! 7.15) and the spec on this base (7.21, draft 7.17 card S-ETA) differ, the
//! spec is the expectation and the difference is marked "S-ETA".
//!
//! No thread, no SimEnv and no real-time wait: every store is built and
//! walked on the calling thread.

#![allow(missing_docs)]
// Test code: the workspace's unwrap/expect/panic allowance covers `#[test]`
// functions only, and the helpers below are called from them alone.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod wal_common;

use nucleus_env::{Env, EnvError, File, MemEnv, OpenMode};
use nucleus_store::StoreError;
use nucleus_store::meta::{Identity, Meta, Ref};
use nucleus_store::wal::append::{Appender, Resume};
use nucleus_store::wal::segment::{
    MIN_WAL_SEG_SIZE, check_seg_size, create_wal, ensure_segment, list_segments,
    parse_segment_name, recycle_below, segment_first, segment_path,
};
use nucleus_store::wal::walk::{Last, StopReason, Walk, WalkEnd, WalkStart, walk};
use nucleus_store::wal::{Body, PREV_NONE, Record, RecordKind, SEGMENT_END_LEN, TxnId};
use wal_common::*;

// ---------------------------------------------------------------------
// Builders: crafted records, placed by the oracle and chained
// ---------------------------------------------------------------------

/// A plan of `Commit`s of boot `boot`, `n` from `n0 + 1`, at `commit_ts`
/// `100 + n`, of the lengths.
fn commits(boot: u32, n0: u64, lens: &[u64]) -> Vec<Plan> {
    lens.iter()
        .enumerate()
        .map(|(i, len)| Plan {
            len: *len,
            boot,
            n: n0 + i as u64 + 1,
            commit_ts: 100 + n0 + i as u64 + 1,
        })
        .collect()
}

/// One crafted record on disk: what it is, and the CRC of the bytes put
/// there (computed by `assemble`, not by the crate).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Crafted {
    lsn: u64,
    len: u64,
    kind: RecordKind,
    boot: u32,
    n: u64,
    commit_ts: u64,
    payload: Vec<u8>,
    crc: u32,
}

impl Crafted {
    /// Its chain pair: the start and the CRC its successor names.
    fn pair(&self) -> (u64, u32) {
        (self.lsn, self.crc)
    }

    /// The chain head after a walk that accepted it.
    fn last(&self) -> Last {
        Last {
            lsn: self.lsn,
            crc: self.crc,
            boot: self.boot,
        }
    }

    /// What a walk's visitor must see for it.
    fn seen(&self) -> Seen {
        // The op's bytes: after the tag and the `u32` length.
        let ops = if self.kind == RecordKind::Commit {
            vec![OwnedOp::Ddl(self.payload[5..].to_vec())]
        } else {
            Vec::new()
        };
        Seen {
            lsn: self.lsn,
            len: self.len as u32,
            kind: self.kind,
            boot: self.boot,
            n: self.n,
            commit_ts: self.commit_ts,
            ops,
        }
    }
}

/// Assembles one record at `lsn`, chained to `prev`, and puts it there.
#[allow(clippy::too_many_arguments)]
fn put_one(
    env: &impl Env,
    lsn: u64,
    prev: (u64, u32),
    kind: RecordKind,
    boot: u32,
    n: u64,
    commit_ts: u64,
    payload: &[u8],
) -> Crafted {
    let bytes = assemble(
        lsn,
        prev.0,
        prev.1,
        kind.byte(),
        boot,
        n,
        commit_ts,
        payload,
    );
    put(env, &bytes);
    Crafted {
        lsn,
        len: bytes.len() as u64,
        kind,
        boot,
        n,
        commit_ts,
        payload: payload.to_vec(),
        crc: crc_of(&bytes),
    }
}

/// Puts a plan from `pos`, chained from `prev`, where the oracle places it,
/// `SegmentEnd`s included; returns every record it put.
fn put_plan<E: Env>(env: &E, pos: u64, prev: (u64, u32), plan: &[Plan]) -> Vec<Crafted> {
    let mut chain = prev;
    oracle_stream(pos, plan)
        .into_iter()
        .map(|o| {
            let c = put_one(
                env,
                o.lsn,
                chain,
                o.kind,
                o.boot,
                o.n,
                o.commit_ts,
                &o.payload,
            );
            chain = c.pair();
            c
        })
        .collect()
}

/// What each visitor of a checked walk saw, as `Crafted`.
fn seen_of(c: &[Crafted]) -> Vec<Seen> {
    c.iter().map(Crafted::seen).collect()
}

/// Checks a whole walk, field by field: how it ended, what its visitor saw,
/// the last accepted record and the newest boot id.
fn check<E: Env>(
    env: &E,
    start: &WalkStart,
    end: WalkEnd,
    accepted: &[Crafted],
    newest_boot: Option<u32>,
) -> Walk {
    let (walk, seen) = collect(env, start).expect("the walk returns");
    let Walk {
        end: got_end,
        accepted: got_accepted,
        last: got_last,
        newest_boot: got_newest,
    } = &walk;
    assert_eq!(*got_end, end, "how the walk ended");
    assert_eq!(*got_accepted, accepted.len() as u64, "how many it accepted");
    assert_eq!(seen, seen_of(accepted), "what its visitor saw");
    assert_eq!(
        *got_last,
        accepted.last().map(Crafted::last),
        "the last accepted record"
    );
    assert_eq!(*got_newest, newest_boot, "the newest boot id");
    walk
}

/// A `SegmentEnd` of `boot` at `lsn`, chained to `prev`.
fn put_end(env: &impl Env, lsn: u64, prev: (u64, u32), boot: u32) -> Crafted {
    put_one(env, lsn, prev, RecordKind::SegmentEnd, boot, 0, 0, &[])
}

/// A `Commit` of `len` bytes, record `i`, of `boot`, `n`, at `commit_ts`.
#[allow(clippy::too_many_arguments)]
fn put_commit(
    env: &impl Env,
    lsn: u64,
    prev: (u64, u32),
    boot: u32,
    n: u64,
    commit_ts: u64,
    len: u64,
    i: u64,
) -> Crafted {
    put_one(
        env,
        lsn,
        prev,
        RecordKind::Commit,
        boot,
        n,
        commit_ts,
        &ddl_payload(len, i),
    )
}

/// Writes bytes at `off` of the file of segment `first`, whatever its own
/// `lsn` says: for the records with a wrong `lsn` and the table's target.
fn put_at<E: Env>(env: &E, first: u64, off: u64, bytes: &[u8]) {
    zeros(env, first);
    let f = env
        .open(&seg_name(first), OpenMode::ReadWrite)
        .expect("open a segment file");
    f.write_at(off, bytes).expect("write bytes at an offset");
}

// ---------------------------------------------------------------------
// t_stale_boot_record (§15.1, I-WAL-STALE; BOOT-ID-STOP-SKIP)
// ---------------------------------------------------------------------

/// S-STORE §12.5's ordered boot-id stop on a crafted stream. Card A-6b,
/// Work 11: (i) a whole record of an older boot; (ii) the same record
/// naming its own history's predecessor; (iii) a `SegmentEnd` of an older
/// boot; (iv) the walk's first record older than the start's boot id;
/// (v) serial-number order, both ways at exactly 2^31; (vi) a walk that
/// crosses into newer boots. BOOT-ID-STOP-SKIP fails (i) to (v).
#[test]
fn t_stale_boot_record() {
    const COMMIT: RecordKind = RecordKind::Commit;
    // (i), (ii), (iii): A1–A3 of boot 5 at 0, 100, 200; at 300 a record of
    // boot 4. The boot-id check comes before the chain check, so a record
    // that names the wrong predecessor is still the same stop.
    for case in 0..3u8 {
        let env = MemEnv::new(1);
        let a = put_plan(&env, 0, PREV_NONE, &commits(5, 0, &[100, 100, 100]));
        match case {
            0 => {
                put_commit(&env, 300, a[2].pair(), 4, 1, 101, 100, 3);
            }
            1 => {
                let payload = ddl_payload(100, 3);
                put_one(&env, 300, (210, 0xDEAD_BEEF), COMMIT, 4, 1, 101, &payload);
            }
            _ => {
                put_end(&env, 300, a[2].pair(), 4);
            }
        }
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Stop {
                valid_end: 300,
                segment: 0,
                reason: StopReason::OlderBoot { found: 4, floor: 5 },
            },
            &a,
            Some(5),
        );
    }
    // (iv) The walk's first record: start `boot_id` 6, A1 of boot 5.
    {
        let env = MemEnv::new(1);
        put_plan(&env, 0, PREV_NONE, &commits(5, 0, &[100, 100]));
        check(
            &env,
            &start(0, PREV_NONE, 6),
            WalkEnd::Stop {
                valid_end: 0,
                segment: 0,
                reason: StopReason::OlderBoot { found: 5, floor: 6 },
            },
            &[],
            None,
        );
    }
    // (v) Serial order: boot 0xFFFF_FFFE, then a newer boot 3, then
    // 0xFFFF_FFFF, which is older than 3 in serial order.
    {
        let env = MemEnv::new(1);
        let a = put_plan(
            &env,
            0,
            PREV_NONE,
            &commits(0xFFFF_FFFE, 0, &[100, 100, 100]),
        );
        let b = put_commit(&env, 300, a[2].pair(), 3, 1, 500, 100, 3);
        put_commit(&env, 400, b.pair(), 0xFFFF_FFFF, 1, 501, 100, 4);
        let mut accepted = a.clone();
        accepted.push(b.clone());
        check(
            &env,
            &start(0, PREV_NONE, 0xFFFF_FFFE),
            WalkEnd::Stop {
                valid_end: 400,
                segment: 0,
                reason: StopReason::OlderBoot {
                    found: 0xFFFF_FFFF,
                    floor: 3,
                },
            },
            &accepted,
            Some(3),
        );
        // Exactly 2^31 apart: each is older than the other.
        let env = MemEnv::new(1);
        let a = put_commit(&env, 0, PREV_NONE, 10, 1, 101, 100, 1);
        put_commit(&env, 100, a.pair(), 10 + (1 << 31), 2, 102, 100, 2);
        check(
            &env,
            &start(0, PREV_NONE, 10),
            WalkEnd::Stop {
                valid_end: 100,
                segment: 0,
                reason: StopReason::OlderBoot {
                    found: 10 + (1 << 31),
                    floor: 10,
                },
            },
            &[a],
            Some(10),
        );
        let env = MemEnv::new(1);
        let a = put_commit(&env, 0, PREV_NONE, 10 + (1 << 31), 1, 101, 100, 1);
        put_commit(&env, 100, a.pair(), 10, 2, 102, 100, 2);
        check(
            &env,
            &start(0, PREV_NONE, 10 + (1 << 31)),
            WalkEnd::Stop {
                valid_end: 100,
                segment: 0,
                reason: StopReason::OlderBoot {
                    found: 10,
                    floor: 10 + (1 << 31),
                },
            },
            &[a],
            Some(10 + (1 << 31)),
        );
    }
    // (vi) A1 (boot 5), A2 and A3 (boot 9), chained: all accepted, then
    // `Magic` at 300; the newest boot id is 9.
    {
        let env = MemEnv::new(1);
        let a1 = put_commit(&env, 0, PREV_NONE, 5, 1, 101, 100, 1);
        let a2 = put_commit(&env, 100, a1.pair(), 9, 2, 102, 100, 2);
        let a3 = put_commit(&env, 200, a2.pair(), 9, 3, 103, 100, 3);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Stop {
                valid_end: 300,
                segment: 0,
                reason: StopReason::Magic,
            },
            &[a1, a2, a3],
            Some(9),
        );
    }
}

// ---------------------------------------------------------------------
// wal_lsn_stop (§15.1, I-WAL-STALE; LSN-CHECK-SKIP)
// ---------------------------------------------------------------------

/// The walk's stop at the first wrong `lsn` (card A-6b, Work 11; §15.1's
/// unnamed A-6b row). (i) A recycled segment's old content; (ii) a record
/// whose `lsn` is 8 past its position; (iii) a recycled record that names
/// its own old predecessor. LSN-CHECK-SKIP fails all three.
#[test]
fn wal_lsn_stop() {
    // (i) and (iii).
    for case in 0..2u8 {
        let env = MemEnv::new(1);
        let a = put_plan(&env, 0, PREV_NONE, &commits(5, 0, &[100, 100]));
        let se = put_end(&env, 200, a[1].pair(), 5);
        // Old content at offset 0 of the recycled segment: a boot-5 record
        // whose `lsn` is 0. In (i) it names the `SegmentEnd` (as the walk's
        // chain would want); in (iii) it names its old predecessor.
        let prev = if case == 0 {
            se.pair()
        } else {
            (0x9999, 0x1234)
        };
        let bytes = assemble(
            0,
            prev.0,
            prev.1,
            RecordKind::Commit.byte(),
            5,
            1,
            101,
            &ddl_payload(100, 9),
        );
        put_at(&env, 1024, 0, &bytes);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Stop {
                valid_end: 1024,
                segment: 1024,
                reason: StopReason::Lsn { found: 0 },
            },
            &[a[0].clone(), a[1].clone(), se],
            Some(5),
        );
    }
    // (ii) A record at 200 whose `lsn` is 208.
    {
        let env = MemEnv::new(1);
        let a = put_plan(&env, 0, PREV_NONE, &commits(5, 0, &[100, 100]));
        let bytes = assemble(
            208,
            a[1].lsn,
            a[1].crc,
            RecordKind::Commit.byte(),
            5,
            3,
            103,
            &ddl_payload(100, 2),
        );
        put_at(&env, 0, 200, &bytes);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Stop {
                valid_end: 200,
                segment: 0,
                reason: StopReason::Lsn { found: 208 },
            },
            &a,
            Some(5),
        );
    }
}

// ---------------------------------------------------------------------
// t_wal_splice (§15.1, I-WAL-CHAIN)
// ---------------------------------------------------------------------

/// Splice (a): the early placement. Returns the start, the expected end,
/// what the walk accepts and the newest boot id.
fn splice_a(env: &impl Env) -> (WalkStart, WalkEnd, Vec<Crafted>, Option<u32>) {
    let h = put_plan(env, 0, PREV_NONE, &commits(5, 0, &[100; 8]));
    let x1 = put_commit(env, 200, h[1].pair(), 5, 6, 206, 120, 11);
    let x2 = put_commit(env, 320, x1.pair(), 5, 7, 207, 80, 12);
    (
        start(0, PREV_NONE, 0),
        WalkEnd::Splice {
            lsn: 400,
            prev_lsn: 300,
            prev_crc: h[3].crc,
            want_prev_lsn: 320,
            want_prev_crc: x2.crc,
        },
        vec![h[0].clone(), h[1].clone(), x1, x2],
        Some(5),
    )
}

/// S-STORE §12.5's splice check on crafted streams (card A-6b, Work 11):
/// (a) the early placement, (b) the late placement, (c) the same-start
/// variant, (d) the same-id residual, (e) same-id, same-start. PREV-CHECK-
/// SKIP fails (a)–(e); PREV-CRC-UNCHECKED fails (c) and (e);
/// PREV-MISMATCH-AS-END fails (a)–(e).
#[test]
fn t_wal_splice() {
    // (a) Early placement.
    {
        let env = MemEnv::new(1);
        let (start, end, accepted, newest) = splice_a(&env);
        check(&env, &start, end, &accepted, newest);
    }
    // (b) Late placement: [500, 700) overwritten the same way.
    {
        let env = MemEnv::new(1);
        let h = put_plan(&env, 0, PREV_NONE, &commits(5, 0, &[100; 8]));
        let x1 = put_commit(&env, 500, h[4].pair(), 5, 6, 206, 120, 11);
        let x2 = put_commit(&env, 620, x1.pair(), 5, 7, 207, 80, 12);
        let mut accepted = h[0..5].to_vec();
        accepted.extend([x1, x2.clone()]);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Splice {
                lsn: 700,
                prev_lsn: 600,
                prev_crc: h[6].crc,
                want_prev_lsn: 620,
                want_prev_crc: x2.crc,
            },
            &accepted,
            Some(5),
        );
    }
    // (c) Same-start: [200, 400) overwritten by two records of length 100
    // with other payload bytes, chained.
    {
        let env = MemEnv::new(1);
        let h = put_plan(&env, 0, PREV_NONE, &commits(5, 0, &[100; 8]));
        let y1 = put_commit(&env, 200, h[1].pair(), 5, 6, 206, 100, 21);
        let y2 = put_commit(&env, 300, y1.pair(), 5, 7, 207, 100, 22);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Splice {
                lsn: 400,
                prev_lsn: 300,
                prev_crc: h[3].crc,
                want_prev_lsn: 300,
                want_prev_crc: y2.crc,
            },
            &[h[0].clone(), h[1].clone(), y1, y2],
            Some(5),
        );
    }
    // (d) Same-id: A1–A5 of boot 7; B1 and B2 overwrite [100, 400); A5 at
    // 400 still names (300, crc(A4)).
    {
        let env = MemEnv::new(1);
        let a = put_plan(&env, 0, PREV_NONE, &commits(7, 0, &[100; 5]));
        let b1 = put_commit(&env, 100, a[0].pair(), 7, 1, 501, 150, 31);
        let b2 = put_commit(&env, 250, b1.pair(), 7, 2, 502, 150, 32);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Splice {
                lsn: 400,
                prev_lsn: 300,
                prev_crc: a[3].crc,
                want_prev_lsn: 250,
                want_prev_crc: b2.crc,
            },
            &[a[0].clone(), b1, b2],
            Some(7),
        );
    }
    // (e) Same-id, same-start.
    {
        let env = MemEnv::new(1);
        let a = put_plan(&env, 0, PREV_NONE, &commits(7, 0, &[100; 5]));
        let b1 = put_commit(&env, 100, a[0].pair(), 7, 1, 501, 100, 41);
        let b2 = put_commit(&env, 200, b1.pair(), 7, 2, 502, 100, 42);
        let b3 = put_commit(&env, 300, b2.pair(), 7, 3, 503, 100, 43);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Splice {
                lsn: 400,
                prev_lsn: 300,
                prev_crc: a[3].crc,
                want_prev_lsn: 300,
                want_prev_crc: b3.crc,
            },
            &[a[0].clone(), b1, b2, b3],
            Some(7),
        );
    }
}

/// Assembles one record at `lsn`, chained to `prev`: its bytes, and what
/// it is. It does not write it.
fn make(
    lsn: u64,
    prev: (u64, u32),
    kind: RecordKind,
    boot: u32,
    n: u64,
    commit_ts: u64,
    payload: &[u8],
) -> (Vec<u8>, Crafted) {
    let bytes = assemble(
        lsn,
        prev.0,
        prev.1,
        kind.byte(),
        boot,
        n,
        commit_ts,
        payload,
    );
    let what = Crafted {
        lsn,
        len: bytes.len() as u64,
        kind,
        boot,
        n,
        commit_ts,
        payload: payload.to_vec(),
        crc: crc_of(&bytes),
    };
    (bytes, what)
}

/// Assembles one record with a raw kind byte, for the kinds that are not
/// record kinds; it does not write it.
fn make_raw(
    lsn: u64,
    prev: (u64, u32),
    kind: u8,
    boot: u32,
    n: u64,
    commit_ts: u64,
    payload: &[u8],
) -> (Vec<u8>, u32) {
    let bytes = assemble(lsn, prev.0, prev.1, kind, boot, n, commit_ts, payload);
    let crc = crc_of(&bytes);
    (bytes, crc)
}

/// How a walk ended, without the text of a `Malformed` detail: the five
/// ways, each destructured in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// It stopped at a valid end.
    Stop {
        valid_end: u64,
        segment: u64,
        reason: StopReason,
    },
    /// A splice.
    Splice {
        lsn: u64,
        prev_lsn: u64,
        prev_crc: u32,
        want_prev_lsn: u64,
        want_prev_crc: u32,
    },
    /// A record that does not decode.
    Malformed { lsn: u64 },
    /// The start segment is missing.
    StartSegmentMissing { wal_start_lsn: u64 },
    /// A read past a segment file's end-of-file that is not a hop's stop.
    ShortRead {
        lsn: u64,
        segment: u64,
        file_len: u64,
    },
}

/// A valid end with its reason.
fn stop(valid_end: u64, segment: u64, reason: StopReason) -> Outcome {
    Outcome::Stop {
        valid_end,
        segment,
        reason,
    }
}

/// The walk's end, with every field of every variant read and no wildcard
/// (a sixth variant, or a field added to one, fails to compile here).
fn outcome(end: &WalkEnd) -> Outcome {
    match end {
        WalkEnd::Stop {
            valid_end,
            segment,
            reason,
        } => Outcome::Stop {
            valid_end: *valid_end,
            segment: *segment,
            reason: *reason,
        },
        WalkEnd::Splice {
            lsn,
            prev_lsn,
            prev_crc,
            want_prev_lsn,
            want_prev_crc,
        } => Outcome::Splice {
            lsn: *lsn,
            prev_lsn: *prev_lsn,
            prev_crc: *prev_crc,
            want_prev_lsn: *want_prev_lsn,
            want_prev_crc: *want_prev_crc,
        },
        WalkEnd::Malformed { lsn, detail } => {
            assert!(
                !detail.is_empty(),
                "a malformed record names what did not decode"
            );
            Outcome::Malformed { lsn: *lsn }
        }
        WalkEnd::StartSegmentMissing { wal_start_lsn } => Outcome::StartSegmentMissing {
            wal_start_lsn: *wal_start_lsn,
        },
        WalkEnd::ShortRead {
            lsn,
            segment,
            file_len,
        } => Outcome::ShortRead {
            lsn: *lsn,
            segment: *segment,
            file_len: *file_len,
        },
    }
}

/// Every stop reason, matched with no wildcard.
fn reason_is_a_stop(reason: &StopReason) -> bool {
    match reason {
        StopReason::Magic
        | StopReason::Length
        | StopReason::Crc
        | StopReason::Lsn { found: _ }
        | StopReason::OlderBoot { found: _, floor: _ }
        | StopReason::ShortSegment
        | StopReason::MissingSegment => true,
    }
}

/// A stop reason's index, matched with no wildcard.
fn reason_index(reason: &StopReason) -> usize {
    match reason {
        StopReason::Magic => 0,
        StopReason::Length => 1,
        StopReason::Crc => 2,
        StopReason::Lsn { found: _ } => 3,
        StopReason::OlderBoot { found: _, floor: _ } => 4,
        StopReason::ShortSegment => 5,
        StopReason::MissingSegment => 6,
    }
}

/// A `Last`, destructured with no `..`.
fn last_is(last: &Last) -> (u64, u32, u32) {
    let Last { lsn, crc, boot } = *last;
    (lsn, crc, boot)
}

/// The walk's decision for one record against an oracle written from
/// S-STORE §12.5 (card A-6b, Work 11), 4,608 cases: the target's kind, the
/// context it sits in (first, after a record, after a `SegmentEnd`, after
/// an exact end), and whether its magic, `len`, CRC, `lsn`, boot id, chain
/// and body are right. The oracle is a function of the dimension values
/// alone and calls nothing in `nucleus_store::wal`. LSN-CHECK-SKIP,
/// BOOT-ID-STOP-SKIP, PREV-CHECK-SKIP, PREV-CRC-UNCHECKED,
/// PREV-MISMATCH-AS-END, STOP-AFTER-CHAIN, WALK-FIRST-PREV-IGNORED,
/// WALK-MALFORMED-AS-STOP and SEG-EXACT-NO-HOP fail it.
#[test]
fn wal_walk_table() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ctx {
        First,
        AfterRecord,
        AfterSegmentEnd,
        AfterExactEnd,
    }
    const CTXS: [Ctx; 4] = [
        Ctx::First,
        Ctx::AfterRecord,
        Ctx::AfterSegmentEnd,
        Ctx::AfterExactEnd,
    ];
    /// The first LSN of the segment every target lies in.
    const SEG: u64 = 2048;
    /// Every context's start pair and boot floor.
    const PAIR: (u64, u32) = (1999, 0x00C0_FFEE);
    const FLOOR: u32 = 10;

    let mut cases = 0u64;
    // How many cases end each way, and with each stop reason.
    let mut ways: [u64; 5] = [0; 5];
    let mut stops_by_reason: [u64; 7] = [0; 7];
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TblKind {
        Commit,
        SegmentEnd,
    }
    for tbl_kind in [TblKind::Commit, TblKind::SegmentEnd] {
        let kind = match tbl_kind {
            TblKind::Commit => RecordKind::Commit,
            TblKind::SegmentEnd => RecordKind::SegmentEnd,
        };
        for ctx in CTXS {
            for bad_magic in [false, true] {
                for len_dim in 0..3u8 {
                    for bad_crc in [false, true] {
                        for bad_lsn in [false, true] {
                            for boot_dim in 0..3u8 {
                                for chain_dim in 0..4u8 {
                                    for bad_body in [false, true] {
                                        cases += 1;
                                        let env = MemEnv::new(cases);
                                        for seg in [1024, 2048, 3072] {
                                            zeros(&env, seg);
                                        }
                                        // The context: where the walk starts,
                                        // the records the oracle accepts before
                                        // the target, the pair the target must
                                        // name, and where the target lies.
                                        let (start_lsn, right, context, p): (
                                            u64,
                                            (u64, u32),
                                            Vec<Crafted>,
                                            u64,
                                        ) = match ctx {
                                            Ctx::First => (SEG, PAIR, Vec::new(), SEG),
                                            Ctx::AfterRecord => {
                                                let a = put_commit(
                                                    &env, SEG, PAIR, FLOOR, 1, 301, 100, 1,
                                                );
                                                (SEG, a.pair(), vec![a], SEG + 100)
                                            }
                                            Ctx::AfterSegmentEnd => {
                                                let a = put_commit(
                                                    &env, 1924, PAIR, FLOOR, 1, 301, 71, 2,
                                                );
                                                let se = put_end(&env, 1995, a.pair(), FLOOR);
                                                (1924, se.pair(), vec![a, se], SEG)
                                            }
                                            Ctx::AfterExactEnd => {
                                                let a = put_commit(
                                                    &env, 1924, PAIR, FLOOR, 1, 301, 124, 3,
                                                );
                                                (1924, a.pair(), vec![a], SEG)
                                            }
                                        };
                                        // The target, from its fields.
                                        let boot = match boot_dim {
                                            0 => FLOOR,
                                            1 => FLOOR + 1,
                                            _ => FLOOR.wrapping_sub(1),
                                        };
                                        let named = match chain_dim {
                                            0 => right,
                                            1 => (right.0 + 1, right.1),
                                            2 => (right.0, right.1 ^ 1),
                                            _ => (right.0 + 1, right.1 ^ 1),
                                        };
                                        let lsn_field = if bad_lsn { p + 8 } else { p };
                                        let (n, commit_ts, payload) = match tbl_kind {
                                            TblKind::Commit => (
                                                1,
                                                100,
                                                if bad_body {
                                                    vec![0u8]
                                                } else {
                                                    ddl_payload(100, 5)
                                                },
                                            ),
                                            TblKind::SegmentEnd => {
                                                (u64::from(bad_body), 0, Vec::new())
                                            }
                                        };
                                        let (mut bytes, target) = make(
                                            lsn_field, named, kind, boot, n, commit_ts, &payload,
                                        );
                                        if bad_magic {
                                            bytes[0] ^= 1;
                                        }
                                        // `len`: right; 52, below a header; `room + 1`, one
                                        // byte past what the target's position leaves in its
                                        // segment (949 after a 100-byte record; 1025 at a
                                        // segment's first LSN).
                                        let room = S + SEG - p;
                                        match len_dim {
                                            1 => bytes[4..8].copy_from_slice(&52u32.to_le_bytes()),
                                            2 => bytes[4..8].copy_from_slice(
                                                &u32::try_from(room + 1)
                                                    .expect("a room in a segment")
                                                    .to_le_bytes(),
                                            ),
                                            _ => {}
                                        }
                                        if bad_crc {
                                            bytes[45] ^= 1;
                                        }
                                        put_at(&env, SEG, p - SEG, &bytes);
                                        // The sentinel: where an accepted target's
                                        // successor lies, naming the target.
                                        let (sentinel_lsn, sentinel) = match tbl_kind {
                                            TblKind::Commit => (
                                                p + 100,
                                                put_commit(
                                                    &env,
                                                    p + 100,
                                                    (p, crc_of(&bytes)),
                                                    boot,
                                                    9,
                                                    302,
                                                    80,
                                                    9,
                                                ),
                                            ),
                                            TblKind::SegmentEnd => (
                                                3072,
                                                put_commit(
                                                    &env,
                                                    3072,
                                                    (p, crc_of(&bytes)),
                                                    boot,
                                                    9,
                                                    302,
                                                    80,
                                                    9,
                                                ),
                                            ),
                                        };
                                        // The oracle: §12.5's order, each check
                                        // after the ones before it.
                                        let older = boot.wrapping_sub(FLOOR) >= 1 << 31;
                                        let expect: Outcome = if bad_magic {
                                            stop(p, SEG, StopReason::Magic)
                                        } else if len_dim != 0 {
                                            stop(p, SEG, StopReason::Length)
                                        } else if bad_crc {
                                            stop(p, SEG, StopReason::Crc)
                                        } else if bad_lsn {
                                            stop(p, SEG, StopReason::Lsn { found: p + 8 })
                                        } else if older {
                                            stop(
                                                p,
                                                SEG,
                                                StopReason::OlderBoot {
                                                    found: boot,
                                                    floor: FLOOR,
                                                },
                                            )
                                        } else if chain_dim != 0 {
                                            Outcome::Splice {
                                                lsn: p,
                                                prev_lsn: named.0,
                                                prev_crc: named.1,
                                                want_prev_lsn: right.0,
                                                want_prev_crc: right.1,
                                            }
                                        } else if bad_body {
                                            Outcome::Malformed { lsn: p }
                                        } else {
                                            // Accepted, then the sentinel, then the
                                            // zeros after it.
                                            stop(
                                                sentinel_lsn + 80,
                                                seg_of(sentinel_lsn),
                                                StopReason::Magic,
                                            )
                                        };
                                        let case = format!(
                                            "case {cases}: {kind:?} in {ctx:?}, magic {bad_magic}, len {len_dim}, \
                                             crc {bad_crc}, lsn {bad_lsn}, boot {boot_dim}, chain {chain_dim}, \
                                             body {bad_body}"
                                        );
                                        let (walk, seen) =
                                            collect(&env, &start(start_lsn, PAIR, FLOOR))
                                                .expect("the walk returns");
                                        let Walk {
                                            end,
                                            accepted: got_accepted,
                                            last: got_last,
                                            newest_boot: got_newest,
                                        } = walk;
                                        let got = outcome(&end);
                                        assert_eq!(got, expect, "{case}");
                                        // The target and the sentinel are accepted exactly
                                        // when the walk ended past the sentinel's start,
                                        // which is never where it stops otherwise.
                                        let accepted_target = match expect {
                                            Outcome::Stop {
                                                valid_end,
                                                segment: _,
                                                reason: _,
                                            } => valid_end > sentinel_lsn,
                                            Outcome::Splice {
                                                lsn: _,
                                                prev_lsn: _,
                                                prev_crc: _,
                                                want_prev_lsn: _,
                                                want_prev_crc: _,
                                            }
                                            | Outcome::Malformed { lsn: _ }
                                            | Outcome::StartSegmentMissing { wal_start_lsn: _ }
                                            | Outcome::ShortRead {
                                                lsn: _,
                                                segment: _,
                                                file_len: _,
                                            } => false,
                                        };
                                        let mut accepted = context.clone();
                                        let mut newest = context.last().map(|c| c.boot);
                                        if accepted_target {
                                            newest = Some(boot);
                                            accepted.extend([target.clone(), sentinel.clone()]);
                                        }
                                        assert_eq!(got_accepted, accepted.len() as u64, "{case}");
                                        assert_eq!(seen, seen_of(&accepted), "{case}");
                                        assert_eq!(
                                            got_last.map(|l| last_is(&l)),
                                            accepted.last().map(|c| last_is(&c.last())),
                                            "{case}"
                                        );
                                        assert_eq!(got_newest, newest, "{case}");
                                        let way = match got {
                                            Outcome::Stop {
                                                reason,
                                                valid_end: _,
                                                segment: _,
                                            } => {
                                                assert!(reason_is_a_stop(&reason));
                                                stops_by_reason[reason_index(&reason)] += 1;
                                                0
                                            }
                                            Outcome::Splice {
                                                lsn: _,
                                                prev_lsn: _,
                                                prev_crc: _,
                                                want_prev_lsn: _,
                                                want_prev_crc: _,
                                            } => 1,
                                            Outcome::Malformed { lsn: _ } => 2,
                                            Outcome::StartSegmentMissing { wal_start_lsn: _ } => 3,
                                            Outcome::ShortRead {
                                                lsn: _,
                                                segment: _,
                                                file_len: _,
                                            } => 4,
                                        };
                                        ways[way] += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 4608);
    // Every way of ending is reached, and every stop reason but a hop's
    // two (no case here hops into a missing or short segment).
    assert!(ways[0] > 0 && ways[1] > 0 && ways[2] > 0);
    assert_eq!((ways[3], ways[4]), (0, 0));
    for (r, count) in stops_by_reason.iter().enumerate() {
        if r < 5 {
            assert!(*count > 0, "stop reason {r} is never reached");
        } else {
            assert_eq!(*count, 0, "stop reason {r} cannot be reached here");
        }
    }
}

// ---------------------------------------------------------------------
// t_wal_short_read (§12.5 "Short segments"; SHORT-READ-AS-END,
// SHORT-HOP-AS-DAMAGE; S-STORE 7.17, card S-ETA)
// ---------------------------------------------------------------------

/// A hop's records: a `SegmentEnd` at 0 in segment 0, then `r2` at 1024
/// and `r3` at 1124 (both 100-byte commits of boot 1), assembled but put
/// only as each case says.
fn hop_records(env: &impl Env) -> (Crafted, Vec<u8>, Vec<u8>, Crafted, Crafted) {
    let se = put_end(env, 0, PREV_NONE, 1);
    let (r2, c2) = make(
        1024,
        se.pair(),
        RecordKind::Commit,
        1,
        1,
        101,
        &ddl_payload(100, 1),
    );
    let (r3, c3) = make(
        1124,
        c2.pair(),
        RecordKind::Commit,
        1,
        2,
        102,
        &ddl_payload(100, 2),
    );
    (se, r2, r3, c2, c3)
}

/// S-STORE §12.5 "Short segments" on crafted segments (S-STORE 7.21 §15.1,
/// A-6b's form of `t_wal_short_read`; card S-ETA). A segment entered by a
/// hop that is missing, empty or shorter than a header at its first LSN
/// ends the walk there with a stop, and one that holds only zeros stops on
/// its magic. A segment entered by a hop and cut short inside its second
/// record's header, inside its second record's body, or exactly at its
/// first record's end, and one whose first record's body runs past its
/// end-of-file, and every short read in the start segment, are damage at
/// that record's start, never a stop. SHORT-READ-AS-END fails the damage
/// cases, and SHORT-HOP-AS-DAMAGE the stops.
#[test]
fn t_wal_short_read() {
    const COMMIT: RecordKind = RecordKind::Commit;
    // The stops: the hop's next segment, missing, empty, shorter than a
    // header, or all zeros.
    let mut stops = 0;
    for case in 0..4u8 {
        let env = MemEnv::new(1);
        let (se, r2, _, _, _) = hop_records(&env);
        let reason = match case {
            0 => {
                // Nothing: the segment is missing.
                StopReason::MissingSegment
            }
            1 => {
                craft(&env, 1024, 0, &[]);
                StopReason::ShortSegment
            }
            2 => {
                craft(&env, 1024, 40, &[(0, &r2[..40])]);
                StopReason::ShortSegment
            }
            _ => {
                zeros(&env, 1024);
                StopReason::Magic
            }
        };
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::Stop {
                valid_end: 1024,
                segment: 1024,
                reason,
            },
            &[se],
            Some(1),
        );
        stops += 1;
    }
    // The damage: the hop's next segment, cut short after a record it holds.
    for (file_len, lsn, accepted) in [
        (140u64, 1124u64, 2usize), // inside the second record's header
        (173, 1124, 2),            // inside the second record's body
        (100, 1124, 2),            // exactly at its first record's end
    ] {
        let env = MemEnv::new(1);
        let (se, r2, r3, c2, _) = hop_records(&env);
        craft(
            &env,
            1024,
            file_len,
            &[(0, &r2), (100, &r3[..(file_len - 100) as usize])],
        );
        let mut records = vec![se, c2];
        records.truncate(accepted);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::ShortRead {
                lsn,
                segment: 1024,
                file_len,
            },
            &records,
            Some(1),
        );
    }
    // ... and whose first record's body runs past its end-of-file.
    {
        let env = MemEnv::new(1);
        let (se, r2, _, _, _) = hop_records(&env);
        craft(&env, 1024, 73, &[(0, &r2[..73])]);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::ShortRead {
                lsn: 1024,
                segment: 1024,
                file_len: 73,
            },
            &[se],
            Some(1),
        );
    }
    // The start segment: a read past its end-of-file is damage at the start
    // itself, and after records the walk accepted there.
    for (file_len, lsn, accepted) in [(30u64, 0u64, 0usize), (100, 100, 1), (173, 100, 1)] {
        let env = MemEnv::new(1);
        let (r1, c1) = make(0, PREV_NONE, COMMIT, 1, 1, 101, &ddl_payload(100, 1));
        let (r2, c2) = make(100, c1.pair(), COMMIT, 1, 2, 102, &ddl_payload(100, 2));
        let parts: [(u64, &[u8]); 2] = [(0, &r1), (100, &r2)];
        craft(&env, 0, file_len, &parts[..accepted + 1]);
        let mut records = vec![c1, c2];
        records.truncate(accepted);
        check(
            &env,
            &start(0, PREV_NONE, 0),
            WalkEnd::ShortRead {
                lsn,
                segment: 0,
                file_len,
            },
            &records,
            if accepted == 0 { None } else { Some(1) },
        );
    }
    assert_eq!(stops, 4);
}

// ---------------------------------------------------------------------
// t_wal_read_error (§12.5 "Read errors"; WAL-READ-ERROR-AS-END)
// ---------------------------------------------------------------------

/// A store that hops into segment 1024: r1 and r2 of length 100 in segment
/// 0, a `SegmentEnd` at 200, and r3 at 1024.
fn hop_store(env: &impl Env) -> Vec<Crafted> {
    let r1 = put_commit(env, 0, PREV_NONE, 1, 1, 101, 100, 1);
    let r2 = put_commit(env, 100, r1.pair(), 1, 2, 102, 100, 2);
    let se = put_end(env, 200, r2.pair(), 1);
    let r3 = put_commit(env, 1024, se.pair(), 1, 3, 103, 100, 3);
    vec![r1, r2, se, r3]
}

/// Whether `e` is the hook's I/O error of `op` on `path`.
fn is_io(e: &StoreError, op: &'static str, path: &str) -> bool {
    match e {
        StoreError::Env(EnvError::Io {
            op: got_op,
            path: got_path,
            kind,
            ..
        }) => *got_op == op && got_path == path && *kind == std::io::ErrorKind::Other,
        _ => false,
    }
}

/// S-STORE §12.5 "Read errors" through an `Env` whose reads of one segment
/// fail (S-STORE 7.21 §15.1, A-6b's form of `t_wal_read_error`; card
/// Work 11's `WALK-READ-ERR-AS-END`). A failing read of a record's header,
/// a failing read of its body, and a failing open of the segment that a hop
/// enters each end the walk with that error, after the records before it,
/// never with a stop or damage; and the walk writes nothing.
/// WAL-READ-ERROR-AS-END (the card's WALK-READ-ERR-AS-END) fails it.
#[test]
fn t_wal_read_error() {
    read_error_cases();
}

// ---------------------------------------------------------------------
// t_wal_malformed (§12.5 "Malformed records"; WAL-MALFORMED-AS-END)
// ---------------------------------------------------------------------

/// S-STORE §12.5 "Malformed records" on crafted streams (S-STORE 7.21
/// §15.1, A-6b's form of `t_wal_malformed`; card Work 11's
/// `WALK-MALFORMED-AS-STOP`). After two accepted records, a record whose
/// magic, `len`, CRC, `lsn`, boot id and back-pointer all pass, but whose
/// kind is 0, whose op tag is unknown, whose op runs past its end, whose
/// last op leaves a byte over, which is a `SegmentEnd` with `n` 1, or a
/// `Chunk` with a nonzero `commit_ts`: each is damage at its start, never a
/// stop, and the walk's visitor never sees it. WAL-MALFORMED-AS-END fails.
#[test]
fn t_wal_malformed() {
    let cases: Vec<(&str, u8, u64, u64, Vec<u8>)> = vec![
        ("kind 0", 0, 1, 103, ddl_payload(100, 3)),
        (
            "an unknown op tag",
            RecordKind::Commit.byte(),
            3,
            103,
            vec![9, 0, 0, 0, 0],
        ),
        (
            "an op running past its end",
            RecordKind::Commit.byte(),
            3,
            103,
            vec![3, 50, 0, 0, 0],
        ),
        (
            "a byte left over",
            RecordKind::Commit.byte(),
            3,
            103,
            vec![3, 0, 0, 0, 0, 0],
        ),
        (
            "a SegmentEnd with n 1",
            RecordKind::SegmentEnd.byte(),
            1,
            0,
            Vec::new(),
        ),
        (
            "a Chunk with a commit_ts",
            RecordKind::Chunk.byte(),
            3,
            5,
            vec![0, 0, 0, 0],
        ),
    ];
    let mut damaged = 0;
    for (what, kind, n, commit_ts, payload) in cases {
        let env = MemEnv::new(13);
        let r1 = put_commit(&env, 0, PREV_NONE, 1, 1, 101, 100, 1);
        let r2 = put_commit(&env, 100, r1.pair(), 1, 2, 102, 100, 2);
        let (bytes, _) = make_raw(200, r2.pair(), kind, 1, n, commit_ts, &payload);
        put(&env, &bytes);
        // The walk: the two records, then damage at 200.
        let (walk, seen) = collect(&env, &start(0, PREV_NONE, 0)).expect("the walk returns");
        let Walk {
            end,
            accepted,
            last,
            newest_boot,
        } = walk;
        assert_eq!(outcome(&end), Outcome::Malformed { lsn: 200 }, "{what}");
        if let WalkEnd::Malformed { lsn, detail } = &end {
            assert_eq!(*lsn, 200);
            assert!(detail.contains("WAL record at LSN 200"), "{what}: {detail}");
        }
        assert_eq!(accepted, 2, "{what}");
        assert_eq!(
            seen,
            seen_of(&[r1, r2.clone()]),
            "{what}: the visitor saw it"
        );
        assert_eq!(
            last.map(|l| last_is(&l)),
            Some(last_is(&r2.last())),
            "{what}"
        );
        assert_eq!(newest_boot, Some(1), "{what}");
        damaged += 1;
    }
    assert_eq!(damaged, 6);
}

// ---------------------------------------------------------------------
// The walk's ends (card test): the stores, each case's expectation
// ---------------------------------------------------------------------

/// The stores of `wal_walk_ends` (a)–(e) and (i), with the walk's start,
/// how it must end, and what it accepts. (a) and (b) are damage under
/// S-STORE 7.17+ (§12.5 "Short segments": a read past the start segment's
/// end-of-file, and after a record the walk accepted in a segment, is
/// never an end); the card, written against 7.15, expected a stop there.
fn ends_store(case: u8, env: &impl Env) -> (WalkStart, Outcome, Vec<Crafted>) {
    const COMMIT: RecordKind = RecordKind::Commit;
    let st = start(0, PREV_NONE, 0);
    match case {
        // (a) 250 bytes: R1 whole, the first 150 bytes of R2 (length 200).
        b'a' => {
            let (r1, c1) = make(0, PREV_NONE, COMMIT, 1, 1, 101, &ddl_payload(100, 1));
            let (r2, _) = make(100, c1.pair(), COMMIT, 1, 2, 102, &ddl_payload(200, 2));
            craft(env, 0, 250, &[(0, &r1), (100, &r2[..150])]);
            (
                st,
                Outcome::ShortRead {
                    lsn: 100,
                    segment: 0,
                    file_len: 250,
                },
                vec![c1],
            )
        }
        // (b) A 200-byte file holding R1, R2 of length 100.
        b'b' => {
            let (r1, c1) = make(0, PREV_NONE, COMMIT, 1, 1, 101, &ddl_payload(100, 1));
            let (r2, c2) = make(100, c1.pair(), COMMIT, 1, 2, 102, &ddl_payload(100, 2));
            craft(env, 0, 200, &[(0, &r1), (100, &r2)]);
            (
                st,
                Outcome::ShortRead {
                    lsn: 200,
                    segment: 0,
                    file_len: 200,
                },
                vec![c1, c2],
            )
        }
        // (c) After an accepted `SegmentEnd`, the next segment's file is
        // missing.
        b'c' => {
            let se = put_end(env, 0, PREV_NONE, 1);
            (st, stop(1024, 1024, StopReason::MissingSegment), vec![se])
        }
        // (c) again, after an exact-end record.
        b'C' => {
            let records = put_plan(env, 0, PREV_NONE, &commits(1, 0, &[256; 4]));
            (st, stop(1024, 1024, StopReason::MissingSegment), records)
        }
        // (d) After a hop, the next file is present and empty.
        b'd' => {
            let se = put_end(env, 0, PREV_NONE, 1);
            craft(env, 1024, 0, &[]);
            (st, stop(1024, 1024, StopReason::ShortSegment), vec![se])
        }
        // (d) again, after an exact-end record.
        b'D' => {
            let records = put_plan(env, 0, PREV_NONE, &commits(1, 0, &[256; 4]));
            craft(env, 1024, 0, &[]);
            (st, stop(1024, 1024, StopReason::ShortSegment), records)
        }
        // (e) R1 (length 100) at 900, then nothing fits (room 24): the walk
        // starts at the record.
        b'e' => {
            let r1 = put_commit(env, 900, PREV_NONE, 1, 1, 101, 100, 1);
            (
                start(900, PREV_NONE, 0),
                stop(1000, 0, StopReason::Length),
                vec![r1],
            )
        }
        // (i) A `Commit` whose payload is op tag 0, chained.
        b'i' => {
            let r1 = put_commit(env, 0, PREV_NONE, 1, 1, 101, 100, 1);
            let (bad, _) = make(100, r1.pair(), COMMIT, 1, 2, 102, &[0]);
            put(env, &bad);
            (st, Outcome::Malformed { lsn: 100 }, vec![r1])
        }
        other => panic!("no such store: {other}"),
    }
}

/// Whether `e` is the walk's refusal of a `seg_size` below the floor.
fn is_invalid(e: &StoreError) -> bool {
    matches!(e, StoreError::Invalid(_))
}

/// The walk's ends (card A-6b, Work 11): short and missing segments after a
/// hop, the tail no record fits in, a file longer than its segment that is
/// never read past it, read errors, a malformed record, the size floor, a
/// visitor that fails, and a start inside a segment. WALK-READ-ERR-AS-END
/// fails (g) and (h); WALK-MALFORMED-AS-STOP fails (i); SHORT-HOP-AS-DAMAGE
/// fails (c) and (d); SHORT-READ-AS-END fails (a) and (b).
#[test]
fn wal_walk_ends() {
    for case in *b"abcCdDei" {
        let env = MemEnv::new(17);
        let (st, expect, accepted) = ends_store(case, &env);
        let (walk, seen) = collect(&env, &st).expect("the walk returns");
        let Walk {
            end,
            accepted: got_accepted,
            last,
            newest_boot,
        } = walk;
        assert_eq!(outcome(&end), expect, "{case}");
        assert_eq!(got_accepted, accepted.len() as u64, "{case}");
        assert_eq!(seen, seen_of(&accepted), "{case}");
        assert_eq!(
            last.map(|l| last_is(&l)),
            accepted.last().map(|c| last_is(&c.last())),
            "{case}"
        );
        assert_eq!(
            newest_boot,
            if accepted.is_empty() { None } else { Some(1) },
            "{case}"
        );
        // Only a stop has a valid end.
        assert_eq!(
            walk_valid_end(&end),
            match expect {
                Outcome::Stop { valid_end, .. } => Some(valid_end),
                _ => None,
            },
            "{case}"
        );
    }
    // (f) A segment file longer than S with bytes past S: never read there.
    {
        let env = HookEnv::new(MemEnv::new(19));
        put_plan(&env, 0, PREV_NONE, &commits(1, 0, &[256; 4]));
        let f = env.open(&seg_name(0), OpenMode::ReadWrite).expect("open");
        f.set_len(2 * S).expect("lengthen");
        f.write_at(S, &[0xA5u8; 64])
            .expect("junk past the segment's end");
        env.forget();
        let (walk, _) = collect(&env, &start(0, PREV_NONE, 0)).expect("the walk returns");
        assert_eq!(
            outcome(&walk.end),
            stop(1024, 1024, StopReason::MissingSegment)
        );
        assert_eq!(
            env.read_offsets(),
            vec![0, 53, 256, 309, 512, 565, 768, 821],
            "the walk read past the segment's end"
        );
        let env = HookEnv::new(MemEnv::new(19));
        put_commit(&env, 900, PREV_NONE, 1, 1, 101, 100, 1);
        let f = env.open(&seg_name(0), OpenMode::ReadWrite).expect("open");
        f.set_len(2 * S).expect("lengthen");
        env.forget();
        let (walk, _) = collect(&env, &start(900, PREV_NONE, 0)).expect("the walk returns");
        assert_eq!(outcome(&walk.end), stop(1000, 0, StopReason::Length));
        assert_eq!(
            env.read_offsets(),
            vec![900, 953],
            "a read where no record fits"
        );
    }
    // (g) and (h): the read errors of `t_wal_read_error`.
    read_error_cases();
    // (j) `seg_size` 211 is `Invalid` before any Env call; 212 walks.
    {
        let env = HookEnv::new(MemEnv::new(23));
        put_commit(&env, 0, PREV_NONE, 1, 1, 101, 58, 1);
        env.forget();
        let err = walk(&env, 211, &start(0, PREV_NONE, 0), &mut |_| Ok(()))
            .expect_err("a size below the floor");
        assert!(is_invalid(&err), "{err}");
        assert!(env.log().is_empty(), "the walk made an Env call");
        let mut visits = 0u64;
        let walk = walk(&env, 212, &start(0, PREV_NONE, 0), &mut |_| {
            visits += 1;
            Ok(())
        })
        .expect("212 walks");
        assert_eq!(visits, 1);
        assert_eq!(
            outcome(&walk.end),
            stop(58, 0, StopReason::Magic),
            "the same file, read in 212-byte segments"
        );
    }
    // (k) A visitor that fails at the third record: the walk returns its
    // error and reads nothing after that record.
    {
        let env = HookEnv::new(MemEnv::new(29));
        let records = put_plan(&env, 0, PREV_NONE, &commits(1, 0, &[100; 4]));
        env.forget();
        let mut visits = 0;
        let err = walk(&env, S, &start(0, PREV_NONE, 0), &mut |_| {
            visits += 1;
            if visits == 3 {
                Err(StoreError::Corrupt("stopped at the third record".into()))
            } else {
                Ok(())
            }
        })
        .expect_err("the visitor's error");
        assert!(
            matches!(&err, StoreError::Corrupt(why) if why == "stopped at the third record"),
            "{err}"
        );
        assert_eq!(visits, 3);
        let offsets = env.read_offsets();
        assert_eq!(
            offsets.last().copied(),
            Some(records[2].lsn + 53),
            "a read after it"
        );
        assert_eq!(offsets.len(), 6);
    }
    // (l) A start in the middle of segment 1: accepted from there.
    {
        let env = MemEnv::new(31);
        let records = put_plan(&env, 1100, (999, 7), &commits(1, 0, &[100; 3]));
        let (walk, seen) = collect(&env, &start(1100, (999, 7), 0)).expect("the walk returns");
        assert_eq!(seen, seen_of(&records));
        assert_eq!(outcome(&walk.end), stop(1400, 1024, StopReason::Magic));
    }
    // (m) `WalkStart::from_meta`: a walk from meta `m` starts at its
    // `wal_start_lsn`, names the pair `wal_start_prev` and
    // `wal_start_prev_crc`, and takes `boot_id` as its floor (§2.2, §14
    // step 3). Every other field is a bystander's.
    {
        let m = Meta {
            identity: Identity {
                format_version: 1,
                page_size: 4096,
                checksum_alg: 1,
                wal_seg_size: 1024,
                creation_uuid: [7; 16],
            },
            ckpt_id: 3,
            t: 900,
            gc_w: 800,
            epoch: 3,
            map_root: Ref::NULL,
            map_level: 1,
            free_root: Ref::NULL,
            dir_root: 7,
            next_lpid: 64,
            slot_count: 4096,
            wal_start_lsn: 4212,
            wal_start_prev: 4043,
            wal_start_prev_crc: 0x5EED_1234,
            wal_ckpt_end: 5000,
            ts_ckpt_hi: 950,
            boot_id: 7,
        };
        assert_eq!(
            WalkStart::from_meta(&m),
            WalkStart {
                lsn: 4212,
                prev_lsn: 4043,
                prev_crc: 0x5EED_1234,
                boot_id: 7,
            }
        );
    }
}

/// `Walk::valid_end` of an end: `Some` for a stop only.
fn walk_valid_end(end: &WalkEnd) -> Option<u64> {
    Walk {
        end: end.clone(),
        accepted: 0,
        last: None,
        newest_boot: None,
    }
    .valid_end()
}

/// The three read-error cases (card Work 11's (g) and (h); the body of
/// `t_wal_read_error`).
fn read_error_cases() {
    let path = seg_name(1024);
    for case in 0..3u8 {
        let env = HookEnv::new(MemEnv::new(11));
        let records = hop_store(&env);
        env.forget();
        let (op, min_read, arm) = match case {
            0 => (
                "read_at",
                0,
                Arm::always("read_at", &path, 0, io_error("read_at", &path)),
            ),
            1 => (
                "read_at",
                53,
                Arm::always("read_at", &path, 53, io_error("read_at", &path)),
            ),
            _ => (
                "open",
                0,
                Arm::always("open", &path, READ, io_error("open", &path)),
            ),
        };
        let before = snapshot(&env);
        env.forget();
        env.arm(arm);
        let mut seen = Vec::new();
        let err = walk_into(&env, &start(0, PREV_NONE, 0), &mut seen).expect_err("a read error");
        assert!(is_io(&err, op, &path), "{case}: {err}");
        // The three records of segment 0 were accepted first.
        assert_eq!(seen, seen_of(&records[..3]), "{case}");
        assert!(
            env.reads_only(),
            "{case}: the walk made a write-class call: {:?}",
            env.log()
        );
        // The failing call is the last: the walk went no further.
        let log = env.log();
        let last = log.last().expect("a logged call");
        assert_eq!((last.op, last.path.as_str()), (op, path.as_str()), "{case}");
        if op == "read_at" {
            assert_eq!(last.a, min_read, "{case}: the failing read's offset");
        }
        env.disarm();
        assert_eq!(snapshot(&env), before, "{case}: the walk wrote something");
        // Disarmed, the same walk accepts everything.
        env.disarm();
        let (walk, seen) = collect(&env, &start(0, PREV_NONE, 0)).expect("the walk returns");
        assert_eq!(seen, seen_of(&records), "{case}");
        assert_eq!(
            outcome(&walk.end),
            stop(1124, 1024, StopReason::Magic),
            "{case}"
        );
    }
}

// ---------------------------------------------------------------------
// wal_walk_writes_nothing (card test; §14.1)
// ---------------------------------------------------------------------

/// Splice case (a) over a hook environment.
fn splice_a_hook(env: &HookEnv<MemEnv>) -> (WalkStart, Outcome, Vec<Crafted>) {
    let (st, end, accepted, _) = splice_a(env);
    (st, outcome(&end), accepted)
}

/// `t_stale_boot_record` case (i) over a hook environment.
fn stale_i_hook(env: &HookEnv<MemEnv>) -> (WalkStart, Outcome, Vec<Crafted>) {
    let a = put_plan(env, 0, PREV_NONE, &commits(5, 0, &[100, 100, 100]));
    put_commit(env, 300, a[2].pair(), 4, 1, 101, 100, 3);
    (
        start(0, PREV_NONE, 0),
        stop(300, 0, StopReason::OlderBoot { found: 4, floor: 5 }),
        a,
    )
}

/// The walk writes nothing (card A-6b, Work 11; §14.1 "What it refuses"):
/// over the stores of `wal_walk_ends` (a)–(e), (i), `t_wal_splice` (a) and
/// `t_stale_boot_record` (i), the hook's log holds only `open` with
/// `OpenMode::Read` and `read_at`, and every file under `wal/` is
/// byte-identical, length included, before and after the walk; over an
/// `Env` that refuses every write-class call the same walks give the same
/// `Walk`.
#[test]
fn wal_walk_writes_nothing() {
    type Built = (WalkStart, Outcome, Vec<Crafted>);
    type Builder = fn(&HookEnv<MemEnv>) -> Built;
    let builders: [(&str, Builder); 10] = [
        ("ends (a)", |e| ends_store(b'a', e)),
        ("ends (b)", |e| ends_store(b'b', e)),
        ("ends (c)", |e| ends_store(b'c', e)),
        ("ends (c, exact end)", |e| ends_store(b'C', e)),
        ("ends (d)", |e| ends_store(b'd', e)),
        ("ends (d, exact end)", |e| ends_store(b'D', e)),
        ("ends (e)", |e| ends_store(b'e', e)),
        ("ends (i)", |e| ends_store(b'i', e)),
        ("splice (a)", splice_a_hook),
        ("stale (i)", stale_i_hook),
    ];
    for (name, build) in builders {
        let mem = MemEnv::new(37);
        let plain = HookEnv::new(mem.clone());
        let (st, expect, accepted) = build(&plain);
        let before = snapshot(&plain);
        plain.forget();
        let (walk, seen) = collect(&plain, &st).expect("the walk returns");
        // Only now, while the log holds the walk's calls alone.
        assert!(
            plain.reads_only(),
            "{name}: a call that is not a read: {:?}",
            plain.log()
        );
        assert_eq!(outcome(&walk.end), expect, "{name}");
        assert_eq!(seen, seen_of(&accepted), "{name}");
        assert_eq!(snapshot(&plain), before, "{name}: a file changed");
        // Every open was read-only, by name.
        for (path, mode) in plain.opens() {
            assert_eq!(mode, READ, "{name}: {path} was opened in mode {mode}");
            assert!(
                path.starts_with("wal/") && path.ends_with(".log"),
                "{name}: the walk opened {path}"
            );
        }
        for call in plain.log() {
            if call.op == "read_at" {
                assert!(
                    call.path.starts_with("wal/"),
                    "{name}: a read of {}",
                    call.path
                );
            }
        }
        // Over the refusing `Env`: the same walk, the same answer.
        let refusing = HookEnv::refusing_writes(mem);
        refusing.forget();
        let (walk2, seen2) = collect(&refusing, &st).expect("the walk returns");
        assert_eq!(walk2, walk, "{name}");
        assert_eq!(seen2, seen, "{name}");
        assert!(refusing.reads_only(), "{name}");
    }
}

// ---------------------------------------------------------------------
// wal_segment_names (card test; §12.1)
// ---------------------------------------------------------------------

/// Segment names and the size floor (card A-6b, Work 11; §12.1): the
/// path's 16 lowercase hex digits, `parse_segment_name`'s round trip and
/// refusals, `list_segments` skipping what is not a segment, and
/// `check_seg_size`'s floor and ceiling.
#[test]
fn wal_segment_names() {
    const BAD_NAMES: [&str; 8] = [
        "000000000000000A.log",  // uppercase hex
        "000000000000000.log",   // 15 digits
        "00000000000000000.log", // 17 digits
        "0000000000000000.txt",  // another suffix
        "000000000000000g.log",  // not a hex digit
        "0000000000000000",      // no suffix
        "foo",
        "",
    ];
    assert_eq!(segment_path(0), "wal/0000000000000000.log");
    assert_eq!(segment_path(0xABC), "wal/0000000000000abc.log");
    assert_eq!(segment_path(u64::MAX), "wal/ffffffffffffffff.log");
    for first in [0u64, 1, 0xABC, 4096, u64::MAX] {
        let path = segment_path(first);
        let base = path.strip_prefix("wal/").expect("the directory");
        assert_eq!(parse_segment_name(base), Some(first), "{base}");
    }
    for bad in BAD_NAMES {
        assert_eq!(parse_segment_name(bad), None, "{bad:?}");
    }
    assert_eq!(SEGMENT_END_LEN, 53);
    assert_eq!(MIN_WAL_SEG_SIZE, 212);
    assert_eq!(
        (
            segment_first(0, S),
            segment_first(1023, S),
            segment_first(1024, S)
        ),
        (0, 0, 1024)
    );
    assert_eq!(
        (
            segment_first(211, 212),
            segment_first(212, 212),
            segment_first(213, 212)
        ),
        (0, 212, 212)
    );
    for good in [212u64, 1024, u64::from(u32::MAX)] {
        assert!(check_seg_size(good).is_ok(), "{good}");
    }
    for bad in [0u64, 211, u64::from(u32::MAX) + 1, u64::MAX] {
        assert!(
            is_invalid(&check_seg_size(bad).expect_err("refused")),
            "{bad}"
        );
    }
    // `list_segments`: only segment names whose first LSN is a multiple of S.
    let env = MemEnv::new(1);
    wal_dir(&env);
    zeros(&env, 3072);
    env.open("wal/foo", OpenMode::CreateNew)
        .expect("a file that is not a segment");
    zeros(&env, 0);
    env.open("wal/0000000000000005.log", OpenMode::CreateNew)
        .expect("a misnamed file");
    zeros(&env, 2048);
    assert_eq!(list_segments(&env, S).expect("list"), vec![0, 2048, 3072]);
    // Another segment size sees other names as segments.
    assert_eq!(
        list_segments(&env, 1024).expect("list"),
        vec![0, 2048, 3072]
    );
    // A size below the floor is refused before any I/O.
    let hooked = HookEnv::new(MemEnv::new(1));
    assert!(is_invalid(
        &list_segments(&hooked, 211).expect_err("refused")
    ));
    assert!(hooked.log().is_empty());
}

/// Reads `len` bytes at `off` of the file of segment `seg`, through `env`.
fn read_at<E: Env>(env: &E, seg: u64, off: u64, len: usize) -> Vec<u8> {
    let f = env
        .open(&seg_name(seg), OpenMode::Read)
        .expect("open a segment");
    let mut b = vec![0u8; len];
    let mut at = 0usize;
    while at < len {
        let n = f.read_at(off + at as u64, &mut b[at..]).expect("read");
        assert!(n > 0, "a short read before the end of {seg}");
        at += n;
    }
    b
}

// ---------------------------------------------------------------------
// wal_append_tails: the writer's tail, every room (SEG-TAIL-SHORT,
// PREV-HOP-WRONG)
// ---------------------------------------------------------------------

/// The writer's tail in every room (card A-6b, Work 11; §12.2 "Records
/// never span segments"): for each record length in {58, 256} and every
/// room from 53 to 330, one appended `Commit` lands where the oracle puts
/// it, a hop writes a `SegmentEnd` of the boot that names the chain head,
/// the record names its true predecessor, and a walk from the old position
/// accepts exactly those records and stops on the zeros after them. A room
/// of 1 to 52 is `Invalid` at `new`. 556 cases. SEG-TAIL-SHORT (on A-6a's
/// `place`) fails the rooms `L + 1` to `L + 52`; PREV-HOP-WRONG (a) fails
/// every hopped case, (b) every hopped case but room 53.
#[test]
fn wal_append_tails() {
    let mut cases = 0u64;
    let mut hopped = 0u64;
    for len in [58u64, 256] {
        for room in 53..=330u64 {
            cases += 1;
            let env = MemEnv::new(41);
            zeros(&env, 1024);
            zeros(&env, 2048);
            let pos = 2048 - room;
            let mut app = Appender::new(
                &env,
                S,
                1,
                Resume {
                    append_pos: pos,
                    prev_lsn: 7,
                    prev_crc: 7,
                    durable_end: pos,
                },
            )
            .expect("new");
            let appended = app
                .append(&commit(1, 1, 101, &ddl_payload(len, 1)))
                .expect("the append");
            let (segment_end, lsn) = oracle_place(pos, len);
            let end = lsn + len;
            let mut accepted = Vec::new();
            match segment_end {
                None => {
                    assert_eq!(
                        (appended.lsn, appended.segment_end, appended.end),
                        (pos, None, end),
                        "len {len}, room {room}"
                    );
                    // The room left after it is 0 or at least a `SegmentEnd`'s.
                    let left = 2048 - end;
                    assert!(
                        left == 0 || left >= 53,
                        "len {len}, room {room}: {left} left"
                    );
                    let want = assemble(
                        pos,
                        7,
                        7,
                        RecordKind::Commit.byte(),
                        1,
                        1,
                        101,
                        &ddl_payload(len, 1),
                    );
                    assert_eq!(read_at(&env, 1024, pos - 1024, len as usize), want);
                    assert_eq!(appended.crc, crc_of(&want));
                    accepted.push(Seen {
                        lsn: pos,
                        len: len as u32,
                        kind: RecordKind::Commit,
                        boot: 1,
                        n: 1,
                        commit_ts: 101,
                        ops: vec![OwnedOp::Ddl(ddl_payload(len, 1)[5..].to_vec())],
                    });
                    // Nothing else was written into the segment.
                    let before = read_at(&env, 1024, 0, (pos - 1024) as usize);
                    assert!(before.iter().all(|b| *b == 0), "bytes before the record");
                    let after = read_at(&env, 1024, end - 1024, (2048 - end) as usize);
                    assert!(after.iter().all(|b| *b == 0), "bytes after the record");
                }
                Some(q) => {
                    hopped += 1;
                    assert_eq!((q, lsn), (pos, 2048), "len {len}, room {room}");
                    let want_end = assemble(q, 7, 7, RecordKind::SegmentEnd.byte(), 1, 0, 0, &[]);
                    assert_eq!(read_at(&env, 1024, q - 1024, HEADER), want_end);
                    let want = assemble(
                        2048,
                        q,
                        crc_of(&want_end),
                        RecordKind::Commit.byte(),
                        1,
                        1,
                        101,
                        &ddl_payload(len, 1),
                    );
                    assert_eq!(read_at(&env, 2048, 0, len as usize), want);
                    assert_eq!(appended.segment_end, Some((q, crc_of(&want_end))));
                    assert_eq!(
                        (appended.lsn, appended.end, appended.crc),
                        (2048, 2048 + len, crc_of(&want)),
                        "len {len}, room {room}"
                    );
                    accepted.push(Seen {
                        lsn: q,
                        len: 53,
                        kind: RecordKind::SegmentEnd,
                        boot: 1,
                        n: 0,
                        commit_ts: 0,
                        ops: Vec::new(),
                    });
                    accepted.push(Seen {
                        lsn: 2048,
                        len: len as u32,
                        kind: RecordKind::Commit,
                        boot: 1,
                        n: 1,
                        commit_ts: 101,
                        ops: vec![OwnedOp::Ddl(ddl_payload(len, 1)[5..].to_vec())],
                    });
                }
            }
            assert_eq!(
                (
                    app.position(),
                    app.written_end(),
                    app.durable_end(),
                    app.chain_head(),
                ),
                (end, end, pos, (lsn, appended.crc)),
                "len {len}, room {room}"
            );
            // The walk from the old position.
            let st = start(pos, (7, 7), 1);
            let (walk, seen) = collect(&env, &st).expect("the walk returns");
            assert_eq!(seen, accepted, "len {len}, room {room}");
            // It stops on the zeros after the record, at the record's end or,
            // after an exact end, at the next segment's first LSN.
            assert_eq!(
                outcome(&walk.end),
                stop(end, seg_of(end), StopReason::Magic),
                "len {len}, room {room}"
            );
            assert_eq!(walk.chain_head(&st), (lsn, appended.crc));
            assert_eq!(walk.accepted, accepted.len() as u64);
        }
    }
    assert_eq!(cases, 556);
    // By the oracle: 57 hopped rooms for a 58-byte record (53 to 110 but
    // 58) and 255 for a 256-byte one (53 to 308 but 256).
    assert_eq!(hopped, 57 + 255);
    // Every room of 1 to 52 is refused at `new`, before any I/O.
    for room in 1..=52u64 {
        let env = HookEnv::new(MemEnv::new(41));
        assert!(is_invalid(
            &Appender::new(
                &env,
                S,
                1,
                Resume {
                    append_pos: 2048 - room,
                    prev_lsn: 7,
                    prev_crc: 7,
                    durable_end: 2048 - room,
                },
            )
            .expect_err("a tail shorter than a SegmentEnd")
        ));
        assert!(env.log().is_empty(), "room {room}");
    }
}

/// The `EnvError` of a `StoreError::Env`, or a panic naming it.
fn env_err(e: &StoreError) -> &EnvError {
    match e {
        StoreError::Env(e) => e,
        other => panic!("not an Env error: {other}"),
    }
}

/// A logged call.
fn call(op: &'static str, path: &str) -> Call {
    Call {
        op,
        path: path.to_string(),
        a: 0,
        b: 0,
    }
}

/// A logged call with `a`.
fn call_a(op: &'static str, path: &str, a: u64) -> Call {
    Call {
        a,
        ..call(op, path)
    }
}

/// A logged call with `a` and `b`.
fn call_ab(op: &'static str, path: &str, a: u64, b: u64) -> Call {
    Call {
        a,
        b,
        ..call(op, path)
    }
}

/// A hook environment over a MemEnv with the WAL created in it.
fn fresh_wal(seed: u64) -> HookEnv<MemEnv> {
    let env = HookEnv::new(MemEnv::new(seed));
    create_wal(&env, S).expect("create_wal");
    env
}

/// A creating session's resume point.
const CREATING: Resume = Resume {
    append_pos: 0,
    prev_lsn: u64::MAX,
    prev_crc: 0,
    durable_end: 0,
};

// ---------------------------------------------------------------------
// wal_append_errors (card test; APPEND-ERR-RETRY)
// ---------------------------------------------------------------------

/// The appender's errors over a hook environment (card A-6b, Work 11): (a) a
/// failing `sync_data`, (b) a failing `write_at`: each is returned, then
/// latched, with no later `Env` call and `durable_end` unmoved; (c) a
/// `set_len` that fails with `NoSpace` while a new segment is made usable;
/// (d) misuse is `Invalid` and leaves the appender where it was.
/// APPEND-ERR-RETRY fails (a) and (b).
#[test]
fn wal_append_errors() {
    let ddl = ddl_payload(100, 1);
    let rec = commit(1, 1, 101, &ddl);
    // (a) `sync_data` of segment 0 fails once.
    {
        let env = fresh_wal(43);
        let mut app = Appender::new(&env, S, 1, CREATING).expect("new");
        let first = app.append(&rec).expect("the first append");
        env.forget();
        let p0 = seg_name(0);
        env.arm(Arm::once("sync_data", &p0, 0, io_error("sync_data", &p0)));
        let err = app.sync().expect_err("the sync fails");
        assert!(is_io(&err, "sync_data", &p0));
        let log = env.log();
        assert_eq!(
            log,
            vec![call("sync_data", &p0)],
            "the failing call is logged"
        );
        // A second `sync`, and a later `append`, return the same error, and
        // nothing reaches the `Env`.
        assert_eq!(env_err(&app.sync().expect_err("latched")), env_err(&err));
        assert_eq!(
            env_err(&app.append(&rec).expect_err("latched")),
            env_err(&err)
        );
        assert_eq!(env.log(), log, "a call after the failing one");
        assert_eq!(app.durable_end(), 0);
        assert_eq!(
            (app.position(), app.written_end(), app.chain_head()),
            (100, 100, (0, first.crc))
        );
    }
    // (b) A failing `write_at`.
    {
        let env = fresh_wal(43);
        let mut app = Appender::new(&env, S, 1, CREATING).expect("new");
        app.append(&rec).expect("the first append");
        env.forget();
        let p0 = seg_name(0);
        env.arm(Arm::once("write_at", &p0, 0, io_error("write_at", &p0)));
        let err = app.append(&rec).expect_err("the write fails");
        assert!(is_io(&err, "write_at", &p0));
        let log = env.log();
        assert_eq!(log, vec![call_ab("write_at", &p0, 100, 100)]);
        assert_eq!(env_err(&app.sync().expect_err("latched")), env_err(&err));
        assert_eq!(
            env_err(&app.append(&rec).expect_err("latched")),
            env_err(&err)
        );
        assert_eq!(env.log(), log, "a call after the failing one");
        assert_eq!((app.durable_end(), app.position()), (0, 100));
    }
    // (c) `set_len` fails with `NoSpace` while a new segment is made usable.
    {
        let env = fresh_wal(43);
        let pos = 1024 - 53;
        let mut app = Appender::new(
            &env,
            S,
            1,
            Resume {
                append_pos: pos,
                prev_lsn: 7,
                prev_crc: 7,
                durable_end: pos,
            },
        )
        .expect("new");
        env.forget();
        let p1 = seg_name(1024);
        env.arm(Arm::always("set_len", &p1, 0, no_space("set_len", &p1)));
        let err = app.append(&rec).expect_err("the set_len fails");
        assert_eq!(env_err(&err), &no_space("set_len", &p1));
        let log = env.log();
        assert_eq!(
            log.last().map(|c| (c.op, c.path.as_str())),
            Some(("set_len", p1.as_str()))
        );
        // Latched: nothing more reaches the `Env`, and `durable_end` stays.
        assert_eq!(
            env_err(&app.append(&rec).expect_err("latched")),
            env_err(&err)
        );
        assert_eq!(env_err(&app.sync().expect_err("latched")), env_err(&err));
        assert_eq!(env.log(), log, "a call after the failing one");
        assert_eq!(app.durable_end(), pos);
        // The segment's file stays as short as it was left: 0 bytes.
        assert_eq!(len_of(&env, &p1).expect("the length"), 0);
    }
    // (d) Misuse is `Invalid`, does no I/O, and leaves the appender where it
    // was: a valid append lands where it would have without it.
    {
        let env = fresh_wal(43);
        let mut app = Appender::new(&env, S, 1, CREATING).expect("new");
        env.forget();
        assert!(is_invalid(
            &app.append(&commit(2, 1, 101, &ddl_payload(100, 1)))
                .expect_err("a record of another boot")
        ));
        assert!(is_invalid(
            &app.append(&Record {
                txn: TxnId { boot: 1, n: 1 },
                body: Body::SegmentEnd,
            })
            .expect_err("a SegmentEnd")
        ));
        assert!(is_invalid(
            &app.append(&commit(1, 1, 101, &ddl_payload(257, 1)))
                .expect_err("a record longer than wal_seg_size / 4")
        ));
        assert!(env.log().is_empty(), "steps 1-4 do no I/O");
        let got = app.append(&rec).expect("a valid append after each");
        let plain = fresh_wal(43);
        let mut other = Appender::new(&plain, S, 1, CREATING).expect("new");
        let want = other.append(&rec).expect("append");
        assert_eq!(
            (got.lsn, got.end, got.crc, got.segment_end),
            (want.lsn, want.end, want.crc, want.segment_end)
        );
        assert_eq!(app.durable_end(), 0);
    }
    // (d) again, at `new`: a `durable_end` above the append position, or a
    // `seg_size` below the floor, is `Invalid` before any I/O (§12.4:
    // `durable_end` is never above `written_end`).
    {
        let env = HookEnv::new(MemEnv::new(43));
        assert!(is_invalid(
            &Appender::new(
                &env,
                S,
                1,
                Resume {
                    append_pos: 100,
                    prev_lsn: 7,
                    prev_crc: 7,
                    durable_end: 101,
                },
            )
            .expect_err("durable_end above the append position")
        ));
        assert!(is_invalid(
            &Appender::new(
                &env,
                211,
                1,
                Resume {
                    append_pos: 100,
                    prev_lsn: 7,
                    prev_crc: 7,
                    durable_end: 100,
                },
            )
            .expect_err("a size below the floor")
        ));
        assert!(env.log().is_empty(), "new does no I/O");
    }
}

// ---------------------------------------------------------------------
// wal_op_order: the order of every write-class call (SEG0-LAZY,
// WAL-DIR-NO-SYNC, SEG-ACK-BEFORE-DURABLE, SYNC-OWN-SEGMENTS-ONLY,
// RECYCLE-UNORDERED, RECYCLE-PAST-BOUND)
// ---------------------------------------------------------------------

/// The order of every write-class call, deterministic (card A-6b, Work 11;
/// §2.3 step 2, §12.5's writer rule, §13 step 6): (a) `create_wal`'s exact
/// sequence; (b) an appender makes each segment it writes into usable
/// before its first write there, a new one and a renamed one, once per
/// appender; (c) a resumed appender's first `sync` syncs every segment from
/// `durable_end`'s to `written_end − 1`'s, in order; (d) `recycle_below`'s
/// renames, lowest first, each durable before the next, exactly, and its
/// `Invalid` before any I/O.
#[test]
fn wal_op_order() {
    let p0 = seg_name(0);
    // (a) create_wal.
    {
        let env = HookEnv::new(MemEnv::new(47));
        create_wal(&env, S).expect("create_wal");
        let made_usable = |env: &HookEnv<MemEnv>| -> Vec<Call> {
            // The write-class calls, a creating `open` included (the card's
            // Work 11 (a)); a `ReadWrite` open is not one.
            env.log()
                .into_iter()
                .filter(|c| is_write_class(c.op) || (c.op == "open" && c.a == CREATE))
                .collect()
        };
        assert_eq!(
            made_usable(&env),
            vec![
                call("create_dir", "wal"),
                call("sync_dir", ""),
                call_a("open", &p0, CREATE),
                call_a("set_len", &p0, S),
                call("sync_all", &p0),
                call("sync_dir", "wal"),
            ]
        );
        // The whole log, the `NotFound` `ReadWrite` open included.
        assert_eq!(
            env.ops(),
            vec![
                ("create_dir", "wal".to_string()),
                ("sync_dir", String::new()),
                ("open", p0.clone()),
                ("open", p0.clone()),
                ("len", p0.clone()),
                ("set_len", p0.clone()),
                ("sync_all", p0.clone()),
                ("sync_dir", "wal".to_string()),
            ]
        );
        assert_eq!(env.opens(), vec![(p0.clone(), RW), (p0.clone(), CREATE)]);
        // An existing `wal/` is refused as `AlreadyExists`, and nothing
        // else is called: creation is for a new file.
        env.forget();
        let err = create_wal(&env, S).expect_err("the WAL exists");
        assert!(
            matches!(&err, StoreError::Env(EnvError::AlreadyExists { path }) if path == "wal"),
            "{err}"
        );
        assert_eq!(env.ops(), vec![("create_dir", "wal".to_string())]);
    }
    // `ensure_segment` on its own (card Work 5): a first LSN that is not a
    // multiple of the size is `Invalid` before any I/O; a file longer than
    // the size is left as long as it is (bytes past the size are never read
    // or written) and still made durable before a writer's first write.
    {
        let env = HookEnv::new(MemEnv::new(47));
        let refused = match ensure_segment(&env, S, 5) {
            Err(e) => e,
            Ok(_) => panic!("a first LSN that is not a multiple of the size was accepted"),
        };
        assert!(is_invalid(&refused));
        assert!(env.log().is_empty(), "an Invalid first LSN does no I/O");
        wal_dir(&env);
        let p1 = seg_name(1024);
        env.open(&p1, OpenMode::CreateNew)
            .expect("create")
            .set_len(2 * S)
            .expect("lengthen past the size");
        env.forget();
        let _ = ensure_segment(&env, S, 1024).expect("make usable");
        assert_eq!(
            env.log(),
            vec![
                call_a("open", &p1, RW),
                call("len", &p1),
                call("sync_all", &p1),
                call("sync_dir", "wal"),
            ],
            "no set_len for a file that is long enough"
        );
        assert_eq!(len_of(&env, &p1).expect("the length"), 2 * S);
    }
    // (b) Crossing into a new segment.
    {
        let env = fresh_wal(47);
        let mut app = Appender::new(
            &env,
            S,
            1,
            Resume {
                append_pos: 800,
                prev_lsn: 7,
                prev_crc: 7,
                durable_end: 800,
            },
        )
        .expect("new");
        app.append(&commit(1, 1, 101, &ddl_payload(224, 1)))
            .expect("in place, ending exactly at 1024");
        env.forget();
        let p1 = seg_name(1024);
        app.append(&commit(1, 2, 102, &ddl_payload(100, 2)))
            .expect("the new segment");
        assert_eq!(
            env.log(),
            vec![
                call_a("open", &p1, RW),
                call_a("open", &p1, CREATE),
                call("len", &p1),
                call_a("set_len", &p1, S),
                call("sync_all", &p1),
                call("sync_dir", "wal"),
                call_ab("write_at", &p1, 0, 100),
            ],
            "the segment is made usable before its first write"
        );
        env.forget();
        app.append(&commit(1, 3, 103, &ddl_payload(100, 3)))
            .expect("in the same segment");
        // Each segment is made usable once per appender.
        assert_eq!(env.log(), vec![call_ab("write_at", &p1, 100, 100)]);
    }
    // (b) Crossing into an existing segment, renamed by `recycle_below`: one
    // of full length, and one shorter than S.
    {
        let env = fresh_wal(47);
        craft(&env, 3072, 500, &[]);
        let renamed = recycle_below(&env, S, 4096, 4500).expect("recycle");
        assert_eq!(renamed, vec![(0, 5120), (3072, 6144)]);
        env.forget();
        let p4 = seg_name(4096);
        let p5 = seg_name(5120);
        let p6 = seg_name(6144);
        // Into the full one.
        let mut app = Appender::new(
            &env,
            S,
            1,
            Resume {
                append_pos: 5120 - 53,
                prev_lsn: 7,
                prev_crc: 7,
                durable_end: 5120 - 53,
            },
        )
        .expect("new");
        app.append(&commit(1, 1, 101, &ddl_payload(100, 1)))
            .expect("the renamed segment");
        assert_eq!(
            env.log(),
            vec![
                call_a("open", &p4, RW),
                call_a("open", &p4, CREATE),
                call("len", &p4),
                call_a("set_len", &p4, S),
                call("sync_all", &p4),
                call("sync_dir", "wal"),
                call_ab("write_at", &p4, 971, 53),
                call_a("open", &p5, RW),
                call("len", &p5),
                call("sync_all", &p5),
                call("sync_dir", "wal"),
                call_ab("write_at", &p5, 0, 100),
            ]
        );
        env.forget();
        // Into the short one. The segment it starts in, 5120, exists at full
        // length (the first appender's renamed target): made usable again
        // for this appender, with no `set_len`.
        let mut app = Appender::new(
            &env,
            S,
            1,
            Resume {
                append_pos: 6144 - 53,
                prev_lsn: 7,
                prev_crc: 7,
                durable_end: 6144 - 53,
            },
        )
        .expect("new");
        app.append(&commit(1, 1, 101, &ddl_payload(100, 1)))
            .expect("the short renamed segment");
        assert_eq!(
            env.log(),
            vec![
                call_a("open", &p5, RW),
                call("len", &p5),
                call("sync_all", &p5),
                call("sync_dir", "wal"),
                call_ab("write_at", &p5, 971, 53),
                call_a("open", &p6, RW),
                call("len", &p6),
                call_a("set_len", &p6, S),
                call("sync_all", &p6),
                call("sync_dir", "wal"),
                call_ab("write_at", &p6, 0, 100),
            ],
            "set_len first when the file was shorter than S"
        );
    }
    // (c) A resumed appender's first sync.
    {
        let env = HookEnv::new(MemEnv::new(47));
        let r1 = put_commit(&env, 0, PREV_NONE, 1, 1, 101, 100, 1);
        let r2 = put_commit(&env, 1024, r1.pair(), 1, 2, 102, 100, 2);
        put_commit(&env, 2048, r2.pair(), 1, 3, 103, 100, 3);
        let mut app = Appender::new(
            &env,
            S,
            1,
            Resume {
                append_pos: 2100,
                prev_lsn: 7,
                prev_crc: 7,
                durable_end: 0,
            },
        )
        .expect("new");
        env.forget();
        let p1 = seg_name(1024);
        let p2 = seg_name(2048);
        assert_eq!(app.sync().expect("sync"), 2100);
        assert_eq!(
            env.log(),
            vec![
                call_a("open", &p0, RW),
                call("sync_data", &p0),
                call_a("open", &p1, RW),
                call("sync_data", &p1),
                call_a("open", &p2, RW),
                call("sync_data", &p2),
            ],
            "every segment in [durable_end, written_end), in order"
        );
        assert_eq!(app.durable_end(), 2100);
        // A second sync has nothing to do.
        env.forget();
        assert_eq!(app.sync().expect("sync"), 2100);
        assert!(env.log().is_empty());
    }
    // (d) recycle_below.
    {
        let env = HookEnv::new(MemEnv::new(47));
        for seg in [0u64, 1024, 2048, 3072, 4096] {
            zeros(&env, seg);
        }
        env.forget();
        let renamed = recycle_below(&env, S, 4096, 4500).expect("recycle");
        assert_eq!(
            renamed,
            vec![(0, 5120), (1024, 6144), (2048, 7168), (3072, 8192)]
        );
        assert_eq!(
            env.log(),
            vec![
                call("list_dir", "wal"),
                call(
                    "rename",
                    "wal/0000000000000000.log -> wal/0000000000001400.log"
                ),
                call("sync_dir", "wal"),
                call(
                    "rename",
                    "wal/0000000000000400.log -> wal/0000000000001800.log"
                ),
                call("sync_dir", "wal"),
                call(
                    "rename",
                    "wal/0000000000000800.log -> wal/0000000000001c00.log"
                ),
                call("sync_dir", "wal"),
                call(
                    "rename",
                    "wal/0000000000000c00.log -> wal/0000000000002000.log"
                ),
                call("sync_dir", "wal"),
            ],
            "lowest first, each durable before the next"
        );
        // 4096 is untouched.
        assert_eq!(
            list_segments(&env, S).expect("list"),
            vec![4096, 5120, 6144, 7168, 8192]
        );
        // With bound 3000, only 0 and 1024 move.
        let env = HookEnv::new(MemEnv::new(47));
        for seg in [0u64, 1024, 2048, 3072, 4096] {
            zeros(&env, seg);
        }
        env.forget();
        let renamed = recycle_below(&env, S, 3000, 4500).expect("recycle");
        assert_eq!(
            (
                renamed.len(),
                renamed.iter().map(|r| r.0).collect::<Vec<_>>()
            ),
            (2, vec![0, 1024])
        );
        assert_eq!(
            list_segments(&env, S).expect("list"),
            vec![2048, 3072, 4096, 5120, 6144]
        );
        // A bound above the append position is Invalid with no Env call.
        env.forget();
        assert!(is_invalid(
            &recycle_below(&env, S, 4600, 4500).expect_err("a bound above the append position")
        ));
        assert!(env.log().is_empty());
        // Nothing wholly below the bound: the listing alone, no rename, no
        // directory sync, and no pair.
        let env = HookEnv::new(MemEnv::new(47));
        for seg in [3072u64, 4096] {
            zeros(&env, seg);
        }
        env.forget();
        assert_eq!(
            recycle_below(&env, S, 2048, 4500).expect("nothing to recycle"),
            Vec::<(u64, u64)>::new()
        );
        assert_eq!(env.ops(), vec![("list_dir", "wal".to_string())]);
        assert_eq!(list_segments(&env, S).expect("list"), vec![3072, 4096]);
    }
}
