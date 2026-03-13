// Page Allocation Proof Lemmas

#[cfg(verus_keep_ghost)]
mod proofs {
    // verus! {
    //     /// Lemma: After allocate(), the total is conserved.
    //     proof fn allocate_conserves(
    //         old_allocated: Set<u32>,
    //         old_free: Seq<u32>,
    //         new_allocated: Set<u32>,
    //         new_free: Seq<u32>,
    //         total: nat,
    //         allocated_id: u32,
    //     )
    //         requires
    //             old_allocated.len() + old_free.len() == total,
    //             old_free.contains(allocated_id as int),
    //             !old_allocated.contains(allocated_id),
    //             new_allocated == old_allocated.insert(allocated_id),
    //             new_free.len() == old_free.len() - 1,
    //         ensures
    //             new_allocated.len() + new_free.len() == total,
    //     { }
    //
    //     /// Lemma: After free(), the total is conserved.
    //     proof fn free_conserves(
    //         old_allocated: Set<u32>,
    //         old_free: Seq<u32>,
    //         new_allocated: Set<u32>,
    //         new_free: Seq<u32>,
    //         total: nat,
    //         freed_id: u32,
    //     )
    //         requires
    //             old_allocated.len() + old_free.len() == total,
    //             old_allocated.contains(freed_id),
    //             new_allocated == old_allocated.remove(freed_id),
    //             new_free.len() == old_free.len() + 1,
    //         ensures
    //             new_allocated.len() + new_free.len() == total,
    //     { }
    // }
}
