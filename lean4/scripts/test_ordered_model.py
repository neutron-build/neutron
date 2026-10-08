"""Bounded independent reference for ordered algorithm design, not Lean execution.
Exercises constructor, routing, split propagation and unrelated queries across
all permutations of seven keys and replacement; maps are the independent oracle.
"""
from itertools import permutations
import unittest
# Tree: ('leaf', [(key,value)]) or ('branch', [tree...]).
def entries(t):return t[1] if t[0]=='leaf' else [e for c in t[1] for e in entries(c)]
def minimum(t):return entries(t)[0][0]
def selected(children,k):
    candidates=[c for c in children if minimum(c)<=k]
    return minimum(candidates[-1] if candidates else children[0])
def lookup(t,k):
    if t[0]=='leaf':return next((v for q,v in t[1] if q==k),None)
    key=selected(t[1],k)
    return lookup(next(c for c in t[1] if minimum(c)==key),k)
def step(t,k,v):
    if t[0]=='leaf':
        es=[e for e in t[1] if e[0]<k]+[(k,v)]+[e for e in t[1] if e[0]>k]
        return [('leaf',es)] if len(es)<=2 else [('leaf',es[:1]),('leaf',es[1:])]
    key=selected(t[1],k)
    children=[out for c in t[1] for out in (step(c,k,v) if minimum(c)==key else [c])]
    return [('branch',children)] if len(children)<=3 else [('branch',children[:2]),('branch',children[2:])]
def insert(t,k,v):
    nodes=step(t,k,v)
    return nodes[0] if len(nodes)==1 else ('branch',nodes)
def valid(t):
    es=entries(t)
    if any(es[i][0]>=es[i+1][0] for i in range(len(es)-1)):return None
    if t[0]=='leaf':return 0 if len(es)<=2 else None
    if not 2<=len(t[1])<=3 or any(not entries(c) for c in t[1]):return None
    depths=[valid(c) for c in t[1]]
    return depths[0]+1 if None not in depths and len(set(depths))==1 else None
class OrderedModel(unittest.TestCase):
    def test_bounded_constructor_transitions_and_splits(self):
        for order in permutations(range(7)):
            tree=('leaf',[]);oracle={}
            for key in order:
                tree=insert(tree,key,key+10);oracle[key]=key+10
                self.assertIsNotNone(valid(tree))
                self.assertEqual(dict(entries(tree)),oracle)
                for query in range(-1,8):self.assertEqual(lookup(tree,query),oracle.get(query))
            tree=insert(tree,3,99);oracle[3]=99
            self.assertIsNotNone(valid(tree));self.assertEqual(dict(entries(tree)),oracle)
            for query in range(-1,8):self.assertEqual(lookup(tree,query),oracle.get(query))
    def test_original_first_child_counterexample(self):
        original=('branch',[('leaf',[(1,1)]),('leaf',[(3,3)])])
        self.assertEqual(lookup(original,3),3)
        self.assertIsNone(next((v for k,v in original[1][0][1] if k==3),None))
if __name__=='__main__':unittest.main()
