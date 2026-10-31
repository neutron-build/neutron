#!/usr/bin/env python3
"""Recompute bounded experiment summaries exclusively from saved raw evidence."""
import argparse, collections, json, math
from pathlib import Path


def distribution(values):
    ordered=sorted(values)
    return {'count':len(ordered),**{f'p{p}_ns':ordered[max(0,math.ceil(len(ordered)*p/100)-1)] if ordered else None for p in (50,95,99)}}


def summarize(directory):
    phases=json.loads((directory/'raw.json').read_text())
    rows=[]
    for index in phases:
        phase=json.loads((directory/index['file']).read_text())
        calls=phase['sustained']
        if len(calls)!=index['sustained_calls']: raise AssertionError('raw count mismatch')
        by_worker=collections.defaultdict(list)
        for call in calls:
            if call['elapsed_ns']<=0 or call['offset_ns']<0: raise AssertionError('invalid timing')
            by_worker[call['worker']].append(call)
        for worker,c in by_worker.items():
            if [x['sequence'] for x in c]!=list(range(len(c))): raise AssertionError('worker sequence gap')
            if any(x['operation']!=('transaction-crud' if x['sequence']%4==3 else 'point') for x in c): raise AssertionError('mixed ratio mismatch')
        bins=[]
        duration=phase['sustained_wall_ns']
        for lower in range(0,duration,5_000_000_000):
            upper=min(lower+5_000_000_000,duration)
            selected=[x for x in calls if lower<=x['offset_ns']<upper]
            memory=[x for x in phase['rss_samples'] if lower<=x['offset_ns']<upper]
            bins.append({'start_ns':lower,'end_ns':upper,'completed_calls':len(selected),'consumer_calls_per_second':len(selected)*1e9/(upper-lower),'errors':0,'latency':distribution([x['elapsed_ns'] for x in selected]),'sampled_current_rss_max_bytes':max((x['current_rss_bytes'] for x in memory),default=None)})
        if sum(x['completed_calls'] for x in bins)!=len(calls): raise AssertionError('bin accounting mismatch')
        rows.append({'trial':phase['trial'],'position':phase['position'],'provider':phase['provider'],'startup':phase['startup'],'writes':distribution(phase['writes_ns']),'sustained':{'calls':len(calls),'writes':sum(x['operation']=='transaction-crud' for x in calls),'reads':sum(x['operation']=='point' for x in calls),'consumer_calls_per_second':len(calls)*1e9/duration,'latency_by_operation':{op:distribution([x['elapsed_ns'] for x in calls if x['operation']==op]) for op in ('point','transaction-crud')},'five_second_bins':bins},'parent_rss_scope':'shared-process orchestration only; not a provider footprint score'})
    positions=collections.defaultdict(list)
    for r in rows: positions[r['provider']].append(r['position'])
    if any(sorted(p)!=[0,1,2,3] for p in positions.values()) or len(positions)!=4: raise AssertionError('provider order not balanced')
    return {'scope':'local configured PostgreSQL17; bounded closed-loop soak; no superiority or production capacity conclusion','phases':rows,'total_transaction_samples':sum(r['writes']['count'] for r in rows),'total_sustained_calls':sum(r['sustained']['calls'] for r in rows),'fresh_child_memory_reads':sum(r['startup']['isolated_memory']['reads'] for r in rows),'errors':0}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    with a.output.open('x') as f: json.dump(summarize(a.directory),f,indent=2);f.write('\n')
