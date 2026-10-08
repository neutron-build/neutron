// Finite MVCC interval oracle. Observations contain version IDs, not merely keys.
// Snapshots are fixed per transaction; scans record their actual event positions.
module predicate_history_test {
    type Version = { id:int, key:int, writer:int, born:int, dead:int, deleter:int }
    type Tx = { snapshot:int, commit:int, rank:int }
    type Scan = { tx:int, at:int, low:int, high:int, observed:Set[int] }
    pure def visible(v:Version,t:int):bool = v.born <= t and (v.dead == 0 or t < v.dead)
    pure def matching(v:Version,s:Scan):bool = s.low <= v.key and v.key <= s.high
    pure def result(versions:Set[Version],s:Scan,snapshot:int):Set[int] =
        versions.filter(v => visible(v,snapshot) and matching(v,s)).map(v => v.id)
    pure def valid(txs:int -> Tx,versions:Set[Version],scans:Set[Scan]):bool =
        txs.keys().forall(t => txs.get(t).snapshot < txs.get(t).commit and txs.get(t).rank > 0) and
        tuples(txs.keys(),txs.keys()).forall(((a,b)) => a == b or
            (txs.get(a).rank != txs.get(b).rank and
             (txs.get(a).rank < txs.get(b).rank iff txs.get(a).commit < txs.get(b).commit))) and
        versions.map(v => v.id).size() == versions.size() and
        tuples(versions,versions).forall(((a,b)) => a.id == b.id or a.key != b.key or
            (a.dead > 0 and a.dead <= b.born) or (b.dead > 0 and b.dead <= a.born)) and
        versions.forall(v => v.writer.in(txs.keys()) and v.born == txs.get(v.writer).commit and
            (v.dead == 0 or (v.deleter.in(txs.keys()) and v.dead == txs.get(v.deleter).commit and v.born < v.dead))) and
        scans.forall(s => s.tx.in(txs.keys()) and s.low <= s.high and
            txs.get(s.tx).snapshot <= s.at and s.at < txs.get(s.tx).commit and
            s.observed == result(versions,s,txs.get(s.tx).snapshot))
    pure def edges(txs:int -> Tx,versions:Set[Version],scans:Set[Scan]):Set[(int,int)] =
        tuples(txs.keys(),txs.keys()).filter(((a,b)) => a != b and (
            scans.exists(s => s.tx == b and versions.exists(v => v.writer == a and v.id.in(s.observed))) or
            scans.exists(s => s.tx == a and versions.exists(v => matching(v,s) and
                ((v.writer == b and v.born > txs.get(a).snapshot) or
                 (v.deleter == b and v.dead > txs.get(a).snapshot)))) or
            versions.exists(v => v.writer == a and v.deleter == b and v.dead > 0) or
            versions.exists(v => v.writer == a and versions.exists(w => w.writer == b and v.key == w.key and txs.get(a).rank < txs.get(b).rank))))
    pure def serializable(txs:int -> Tx,versions:Set[Version],scans:Set[Scan]):bool = {
        val graph = edges(txs,versions,scans)
        val reach = txs.keys().fold(graph,(r,k) => r.union(tuples(txs.keys(),txs.keys()).filter(((a,b)) => (a,k).in(r) and (k,b).in(r))))
        valid(txs,versions,scans) and txs.keys().forall(t => not((t,t).in(reach)))
    }
    pure val serial = Map(1 -> {snapshot:0,commit:1,rank:1},2 -> {snapshot:1,commit:4,rank:2})
    pure val rows = Set({id:11,key:1,writer:1,born:1,dead:0,deleter:0},{id:12,key:2,writer:2,born:4,dead:0,deleter:0})
    pure val scans = Set({tx:1,at:0,low:1,high:10,observed:Set()}, {tx:2,at:2,low:1,high:10,observed:Set(11)})
    run conflicting_interval_serial_history_test = serializable(serial,rows,scans) and edges(serial,rows,scans).size() > 0
    run concurrent_empty_range_phantom_witness_test = {
        val txs = serial.set(2,{snapshot:0,commit:4,rank:2})
        val reads = scans.filter(s => s.tx == 1).union(Set({tx:2,at:2,low:1,high:10,observed:Set()}))
        valid(txs,rows,reads) and not(serializable(txs,rows,reads))
    }
    run omitted_visible_row_witness_test = not(valid(serial,rows,scans.map(s => if (s.tx == 2) {...s,observed:Set()} else s)))
    run fabricated_row_witness_test = not(valid(serial,rows,scans.map(s => if (s.tx == 2) {...s,observed:Set(11,999)} else s)))
    run out_of_range_row_witness_test = not(valid(serial,rows,scans.map(s => if (s.tx == 2) {...s,low:5,observed:Set(11)} else s)))
    run repeated_snapshot_insert_test = serializable(serial,rows,scans.union(Set({tx:2,at:3,low:1,high:10,observed:Set(11)})))
    run repeated_insert_delete_snapshot_test = {
        val txs = serial.put(3,{snapshot:1,commit:3,rank:2}).set(2,{snapshot:1,commit:4,rank:3})
        val deleted = rows.map(v => if (v.id == 11) {...v,dead:3,deleter:3} else v)
        // Commit/delete between two reads cannot change a fixed snapshot's result.
        valid(txs,deleted,scans.union(Set({tx:2,at:3,low:1,high:10,observed:Set(11)})))
    }
    run repeated_delete_omission_witness_test = {
        val txs = serial.put(3,{snapshot:1,commit:3,rank:2}).set(2,{snapshot:1,commit:4,rank:3})
        val deleted = rows.map(v => if (v.id == 11) {...v,dead:3,deleter:3} else v)
        not(valid(txs,deleted,scans.union(Set({tx:2,at:3,low:1,high:10,observed:Set()}))))
    }
    run invalid_reads_from_witness_test = not(valid(serial,rows.map(v => if (v.id == 11) {...v,writer:99} else v),scans))
    run duplicate_rank_witness_test = not(valid(serial.set(2,{snapshot:1,commit:4,rank:1}),rows,scans))
    run matching_insert_between_scans_test = {
        val txs=serial.put(3,{snapshot:1,commit:3,rank:2}).set(2,{snapshot:1,commit:4,rank:3})
        val inserted=rows.union(Set({id:15,key:5,writer:3,born:3,dead:0,deleter:0}))
        valid(txs,inserted,scans.union(Set({tx:2,at:3,low:1,high:10,observed:Set(11)})))
    }
    run matching_insert_phantom_read_witness_test = {
        val txs=serial.put(3,{snapshot:1,commit:3,rank:2}).set(2,{snapshot:1,commit:4,rank:3})
        val inserted=rows.union(Set({id:15,key:5,writer:3,born:3,dead:0,deleter:0}))
        not(valid(txs,inserted,scans.union(Set({tx:2,at:3,low:1,high:10,observed:Set(11,15)}))))
    }
}
