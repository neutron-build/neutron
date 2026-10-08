"""Independent finite oracle: enumerate serial schedules rather than graph closure.
These Python checks are oracle regressions, not executed Quint model evidence.
"""
from itertools import permutations, product
import unittest

# Version tuple: id,key,writer,born,dead,deleter; tx: snapshot,commit,rank.
def valid(txs, versions, scans, order):
    if set(order)!=set(txs) or len(order)!=len(txs):return False
    if sorted(txs,key=lambda t:txs[t][1])!=order:return False
    if any(txs[t][2]!=i+1 for i,t in enumerate(order)):return False
    if any(snap>=commit for snap,commit,_ in txs.values()):return False
    if len({v[0] for v in versions})!=len(versions):return False
    for a,b in product(versions,repeat=2):
        if a[0]!=b[0] and a[1]==b[1] and not (a[4] and a[4]<=b[3] or b[4] and b[4]<=a[3]):return False
    for _,_,writer,born,dead,deleter in versions:
        if writer not in txs or born!=txs[writer][1]:return False
        if dead and (deleter not in txs or dead!=txs[deleter][1] or born>=dead):return False
    for tx,at,low,high,observed in scans:
        if tx not in txs or not(txs[tx][0]<=at<txs[tx][1]) or low>high:return False
        expected={i for i,k,_,born,dead,_ in versions if low<=k<=high and born<=txs[tx][0] and (not dead or txs[tx][0]<dead)}
        if observed!=expected:return False
    return True

def serial_oracle(txs,versions,scans,order):
    if not valid(txs,versions,scans,order):return False
    for schedule in permutations(txs):
        rows={}
        for tx in schedule:
            reads=[s for s in scans if s[0]==tx]
            if any(observed!={i for k,i in rows.items() if low<=k<=high} for _,_,low,high,observed in reads):break
            for i,key,writer,_,_,_ in versions:
                if writer==tx:rows[key]=i
            for i,key,_,_,dead,deleter in versions:
                if dead and deleter==tx and rows.get(key)==i:del rows[key]
        else:return True
    return False

class HistoryOracle(unittest.TestCase):
    def setUp(self):
        self.t={1:(0,1,1),2:(1,4,2)}
        self.v=[(11,1,1,1,0,0),(12,2,2,4,0,0)]
        self.s=[(1,0,1,10,set()),(2,2,1,10,{11})]
    def test_serial_conflicting_pair(self):self.assertTrue(serial_oracle(self.t,self.v,self.s,[1,2]))
    def test_original_missing_pair(self):self.assertFalse(serial_oracle(self.t,self.v,self.s,[]))
    def test_duplicate_and_wrong_order(self):
        for order in [[1,1,2],[2,1]]:self.assertFalse(valid(self.t,self.v,self.s,order))
    def test_result_corruptions(self):
        for observed in [set(),{11,999},{12},{999}]:
            self.assertFalse(valid(self.t,self.v,[self.s[0],(2,2,1,10,observed)],[1,2]))
    def test_out_of_range(self):self.assertFalse(valid(self.t,self.v,[self.s[0],(2,2,5,10,{11})],[1,2]))
    def test_snapshot_phantom_cycle(self):
        t={**self.t,2:(0,4,2)};s=[self.s[0],(2,2,1,10,set())]
        self.assertTrue(valid(t,self.v,s,[1,2]));self.assertFalse(serial_oracle(t,self.v,s,[1,2]))
    def test_repeated_snapshot_delete(self):
        t={**self.t,2:(1,4,3),3:(1,3,2)};v=[(11,1,1,1,3,3),self.v[1]]
        scans=self.s+[(2,3,1,10,{11})]
        self.assertTrue(serial_oracle(t,v,scans,[1,3,2]))
        self.assertFalse(valid(t,v,self.s+[(2,3,1,10,set())],[1,3,2]))
    def test_matching_insert_between_scans(self):
        t={**self.t,2:(1,4,3),3:(1,3,2)};v=self.v+[(15,5,3,3,0,0)]
        self.assertTrue(valid(t,v,self.s+[(2,3,1,10,{11})],[1,3,2]))
        self.assertFalse(valid(t,v,self.s+[(2,3,1,10,{11,15})],[1,3,2]))
    def test_invalid_writer_and_rank(self):
        self.assertFalse(valid(self.t,[(11,1,99,1,0,0),self.v[1]],self.s,[1,2]))
        self.assertFalse(valid({**self.t,2:(1,4,1)},self.v,self.s,[1,2]))
    def test_uniform_distribution_and_constant_counterexample(self):
        # Enumerated sample space, not inferred entropy from a string length.
        for domain in range(2,6):
            for draws in range(2,5):
                space=list(product(range(domain),repeat=draws))
                collisions=sum(len(set(s))!=len(s) for s in space)
                pairs=draws*(draws-1)//2
                self.assertLessEqual(collisions*domain,pairs*len(space))
                # Constant/repeated ID support violates uniform pair-event bound.
                self.assertGreater(domain*1,1)
if __name__=='__main__':unittest.main()
