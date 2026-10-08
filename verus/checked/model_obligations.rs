use vstd::prelude::*;
verus! {
pub open spec fn evict(pages: Set<int>, pinned: Set<int>, victim: int) -> Set<int> {
    if pinned.contains(victim) { pages } else { pages.remove(victim) }
}
pub proof fn pinned_survives(pages: Set<int>, pinned: Set<int>, victim: int, id: int)
    requires pinned.subset_of(pages), pinned.contains(id)
    ensures evict(pages, pinned, victim).contains(id)
{ if !pinned.contains(victim) { assert(id != victim); } }

// Two separate outcomes, with admission depending on key overlap and a prior
// commit. Mutating the conflict branch to (true,true) violates the postcondition.
pub fn commit_pair(first_key: u64, second_key: u64) -> (out: (bool, bool))
    ensures out.0, first_key == second_key ==> !out.1,
        first_key != second_key ==> out.1,
        !(out.0 && out.1 && first_key == second_key)
{
    let first_committed = true;
    let second_committed = if first_committed && first_key == second_key { false } else { true };
    (first_committed, second_committed)
}

pub proof fn xor_or_zero(acc:u8,left:u8,right:u8)
    ensures ((acc | (left ^ right)) == 0) == (acc == 0 && left == right)
{ assert(((acc | (left ^ right)) == 0) == (acc == 0 && left == right)) by(bit_vector); }

// Executed byte accesses and a loop-carried accumulator. Equal public lengths
// only; cost counts iterations, not compiler/hardware/wall-clock observations.
// Algorithm mapping: nucleus/src/transport/mod.rs::constant_time_eq_token.
// Mapping is source-pinned in manifest.json; it is not Rust extraction/refinement.
pub fn compare_bytes(left: &Vec<u8>, right: &Vec<u8>) -> (out:(bool,usize))
    requires left.len() == right.len()
    ensures out.1 == left.len(),
        out.0 == (forall|j:int| 0 <= j < left.len() ==> left@[j] == right@[j])
{
    let mut index:usize = 0;
    let mut diff:u8 = 0;
    while index < left.len()
        invariant left.len() == right.len(), index <= left.len(),
            (diff == 0) == (forall|j:int| 0 <= j < index ==> left@[j] == right@[j])
        decreases left.len() - index
    {
        let l = left[index];
        let r = right[index];
        proof { xor_or_zero(diff,l,r); }
        diff = diff | (l ^ r);
        index = index + 1;
    }
    (diff == 0,index)
}

// Joint discrete distribution represented by outcome multiplicities. Every
// ordered value pair has the same mass: this states uniform independent draws
// explicitly, rather than inferring it from a buffer length.
pub open spec fn independent_uniform_pairs(domain:int,sample_space:int,
    pair_count:spec_fn(int,int)->int) -> bool {
    domain > 0 && sample_space > 0 &&
    forall|a:int,b:int| 0 <= a < domain && 0 <= b < domain ==>
        pair_count(a,b) >= 0 && pair_count(a,b)*domain*domain == sample_space
}
pub proof fn constant_distribution_not_uniform(domain:int,sample_space:int,
    pair_count:spec_fn(int,int)->int)
    requires domain > 1, sample_space > 0, pair_count(1,1) == 0
    ensures !independent_uniform_pairs(domain,sample_space,pair_count)
{
    if independent_uniform_pairs(domain,sample_space,pair_count) {
        assert(pair_count(1,1)*domain*domain == sample_space);
        assert(false);
    }
}

// Finite probability space: events are outcome sets; measure is their cardinality
// divided by sample-space size. The executable counting bound uses per-pair
// collision counts and a separately supplied event-cover/subadditivity bound.
// Independent uniform draws must justify per_pair <= sample_space / domain;
// a constant/repeated ID generator cannot discharge this premise for domain > 1.
pub proof fn conditional_collision_bound(sample_space:int,domain:int,pairs:int,
    per_pair:int,collision_outcomes:int)
    requires sample_space > 0, domain > 0, pairs >= 0, per_pair >= 0,
        per_pair * domain <= sample_space,
        0 <= collision_outcomes <= pairs * per_pair
    ensures collision_outcomes * domain <= pairs * sample_space
{
    vstd::arithmetic::mul::lemma_mul_inequality(collision_outcomes,pairs*per_pair,domain);
    vstd::arithmetic::mul::lemma_mul_inequality(per_pair*domain,sample_space,pairs);
    vstd::arithmetic::mul::lemma_mul_is_associative(pairs,per_pair,domain);
}
}
fn main() {}
