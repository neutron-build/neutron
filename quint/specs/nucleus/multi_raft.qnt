// Concrete bounded-domain Raft election/message model, not the shipping Rust.
// Terms are unbounded; logs model full-state AppendEntries delivery. No liveness claim.
module multi_raft {
    import types.* from "../common/types"
    pure val NODES: Set[NodeId] = Set(1, 2, 3)
    pure val GROUPS: Set[GroupId] = Set(1, 2)
    type RaftState = { current_term: Term, voted_for: int, log: List[LogEntry], commit_index: LogIndex, role: str }
    var group_state: (NodeId, GroupId) -> RaftState
    type VoteRequest = { candidate:NodeId, voter:NodeId, group:GroupId, term:Term, last_term:int, length:int }
    type VoteResponse = { candidate:NodeId, voter:NodeId, group:GroupId, term:Term, granted:bool }
    // Queued messages are immutable network payloads, not candidate-state references.
    var requests: Set[VoteRequest]
    var responses: Set[VoteResponse]
    type Snapshot = { leader:NodeId, follower:NodeId, group:GroupId, term:Term, log:List[LogEntry], index:int }
    var snapshots: Set[Snapshot]
    var votes: (NodeId, GroupId) -> Set[NodeId]
    var elected: (GroupId, Term) -> Set[NodeId]
    var alive: Set[NodeId]
    var partitioned: Set[(NodeId,NodeId)]
    pure def log_term(log: List[LogEntry]): int = if (log.length() == 0) 0 else log[log.length()-1].term
    def connected(a: NodeId,b: NodeId): bool = a.in(alive) and b.in(alive) and not((a,b).in(partitioned))
    action init = all {
        group_state' = tuples(NODES,GROUPS).mapBy(((n,g)) => {current_term:0,voted_for:-1,log:List(),commit_index:0,role:"follower"}),
        requests' = Set(), responses' = Set(), snapshots' = Set(), votes' = tuples(NODES,GROUPS).mapBy(x => Set()),
        elected' = Map(), alive' = NODES, partitioned' = Set(),
    }
    action request_vote(node: NodeId, group: GroupId): bool = {
        val st = group_state.get((node,group))
        val term = st.current_term + 1
        all {
            node.in(alive), group.in(GROUPS),
            group_state' = group_state.set((node,group), {...st,current_term:term,voted_for:node,role:"candidate"}),
            requests' = requests.union(NODES.filter(v => v != node).map(v => {candidate:node,voter:v,group:group,term:term,last_term:log_term(st.log),length:st.log.length()})),
            snapshots' = snapshots, responses' = responses,
            votes' = votes.set((node,group),Set(node)),
            elected' = elected, alive' = alive, partitioned' = partitioned,
        }
    }
    // Request receipt persists term/vote before separately queueing a response.
    action deliver_request(req:VoteRequest):bool = {
        val v = group_state.get((req.voter,req.group))
        val granted = req.term >= v.current_term and
            (req.term > v.current_term or v.voted_for == -1 or v.voted_for == req.candidate) and
            (req.last_term > log_term(v.log) or (req.last_term == log_term(v.log) and req.length >= v.log.length()))
        all {
            req.in(requests), connected(req.voter,req.candidate),
            group_state' = group_state.set((req.voter,req.group),
                if (req.term > v.current_term) {...v,current_term:req.term,voted_for:if (granted) req.candidate else -1,role:"follower"}
                else if (granted) {...v,voted_for:req.candidate,role:"follower"} else v),
            requests' = requests.exclude(Set(req)),
            snapshots' = snapshots, responses' = responses.union(Set({candidate:req.candidate,voter:req.voter,group:req.group,term:if (req.term > v.current_term) req.term else v.current_term,granted:granted})),
            votes' = votes, elected' = elected, alive' = alive, partitioned' = partitioned,
        }
    }
    // Compatibility action name: this ONLY delivers a request, never its response.
    action deliver_vote(voter:NodeId,candidate:NodeId,group:GroupId):bool = {
        nondet req = requests.filter(r => r.voter == voter and r.candidate == candidate and r.group == group).oneOf()
        deliver_request(req)
    }
    action receive_response(res:VoteResponse):bool = {
        val c = group_state.get((res.candidate,res.group))
        all {
            res.in(responses), connected(res.voter,res.candidate),
            snapshots' = snapshots, responses' = responses.exclude(Set(res)), requests' = requests,
            group_state' = if (res.term > c.current_term)
                group_state.set((res.candidate,res.group),{...c,current_term:res.term,voted_for:-1,role:"follower"}) else group_state,
            votes' = if (res.granted and res.term == c.current_term and c.role == "candidate")
                votes.set((res.candidate,res.group),votes.get((res.candidate,res.group)).union(Set(res.voter))) else votes,
            elected' = elected, alive' = alive, partitioned' = partitioned,
        }
    }
    action receive_vote(voter:NodeId,candidate:NodeId,group:GroupId):bool = {
        nondet res = responses.filter(r => r.voter == voter and r.candidate == candidate and r.group == group).oneOf()
        receive_response(res)
    }
    action win_election(node: NodeId, group: GroupId): bool = {
        val st = group_state.get((node,group))
        val key = (group,st.current_term)
        val historical = if (key.in(elected.keys())) elected.get(key) else Set()
        all {
            node.in(alive), group.in(GROUPS), st.role == "candidate",
            votes.get((node,group)).size() >= 2,
            group_state' = group_state.set((node,group), {...st,role:"leader"}),
            elected' = elected.put(key,historical.union(Set(node))),
            votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests, alive' = alive, partitioned' = partitioned,
        }
    }
    action append_entry(leader: NodeId, group: GroupId, cmd: str): bool = {
        val st = group_state.get((leader,group))
        all {
            leader.in(alive), group.in(GROUPS), st.role == "leader",
            group_state' = group_state.set((leader,group), {...st,log:st.log.append({term:st.current_term,index:st.log.length()+1,command:cmd})}),
            elected' = elected, votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests, alive' = alive, partitioned' = partitioned,
        }
    }
    // Full prefix delivery is a bounded abstraction of AppendEntries retries.
    action replicate(leader: NodeId, follower: NodeId, group: GroupId): bool = {
        val l = group_state.get((leader,group))
        val f = group_state.get((follower,group))
        all {
            leader != follower, connected(leader,follower), group.in(GROUPS), l.role == "leader", l.current_term >= f.current_term,
            l.log.length() >= f.commit_index,
            0.to(f.commit_index-1).forall(i => l.log[i] == f.log[i]),
            group_state' = group_state.set((follower,group), {...f,current_term:l.current_term,voted_for:if (l.current_term > f.current_term) -1 else f.voted_for,role:"follower",log:l.log}),
            elected' = elected, votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests, alive' = alive, partitioned' = partitioned,
        }
    }
    action step_down(node: NodeId, group: GroupId, term: Term): bool = {
        val st = group_state.get((node,group))
        all { node.in(alive), group.in(GROUPS), term > st.current_term,
            group_state' = group_state.set((node,group), {...st,current_term:term,voted_for:-1,role:"follower"}),
            elected' = elected, votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests, alive' = alive, partitioned' = partitioned,
        }
    }
    action partition(a:NodeId,b:NodeId): bool = all {
        a.in(NODES), b.in(NODES), a != b,
        partitioned' = partitioned.union(Set((a,b),(b,a))), alive' = alive,
        group_state' = group_state, elected' = elected, votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests,
    }
    action heal: bool = all { partitioned' = Set(), alive' = alive,
        group_state' = group_state, elected' = elected, votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests }
    action crash(n:NodeId): bool = all {
        n.in(alive), alive' = alive.exclude(Set(n)), partitioned' = partitioned,
        group_state' = tuples(NODES,GROUPS).mapBy(((node,g)) => val st = group_state.get((node,g))
            if (node == n) {...st,role:"follower"} else st),
        elected' = elected, votes' = tuples(NODES,GROUPS).mapBy(((node,g)) => if (node == n) Set() else votes.get((node,g))), snapshots' = snapshots, responses' = responses, requests' = requests,
    }
    action recover(n:NodeId): bool = all {
        n.in(NODES), not(n.in(alive)), alive' = alive.union(Set(n)), partitioned' = partitioned,
        group_state' = group_state, elected' = elected, votes' = votes, snapshots' = snapshots, responses' = responses, requests' = requests,
    }
    action queue_snapshot(leader:NodeId,follower:NodeId,group:GroupId):bool = {
        val l=group_state.get((leader,group))
        all { leader != follower, l.role == "leader", leader.in(alive), follower.in(NODES), group.in(GROUPS),
            snapshots' = snapshots.union(Set({leader:leader,follower:follower,group:group,term:l.current_term,log:l.log,index:l.log.length()})),
            group_state' = group_state, responses' = responses, requests' = requests,
            votes' = votes, elected' = elected, alive' = alive, partitioned' = partitioned }
    }
    action receive_snapshot(message:Snapshot):bool = {
        val f=group_state.get((message.follower,message.group))
        val compatible = message.log.length() >= f.commit_index and
            0.to(f.commit_index-1).forall(i => message.log[i] == f.log[i])
        all { message.in(snapshots), connected(message.leader,message.follower),
            snapshots' = snapshots.exclude(Set(message)),
            group_state' = if (message.term >= f.current_term and message.index > f.commit_index and
                message.index == message.log.length() and compatible)
                group_state.set((message.follower,message.group),{...f,current_term:message.term,
                    voted_for:if (message.term > f.current_term) -1 else f.voted_for,
                    role:"follower",log:message.log,commit_index:message.index}) else group_state,
            responses' = responses, requests' = requests, votes' = votes,
            elected' = elected, alive' = alive, partitioned' = partitioned }
    }
    action step = any {
        { nondet n=NODES.oneOf() nondet g=GROUPS.oneOf() request_vote(n,g) },
        { nondet n=NODES.oneOf() nondet c=NODES.oneOf() nondet g=GROUPS.oneOf() deliver_vote(n,c,g) },
        { nondet n=NODES.oneOf() nondet c=NODES.oneOf() nondet g=GROUPS.oneOf() receive_vote(n,c,g) },
        { nondet n=NODES.oneOf() nondet g=GROUPS.oneOf() win_election(n,g) },
        { nondet n=NODES.oneOf() nondet g=GROUPS.oneOf() append_entry(n,g,"cmd") },
        { nondet n=NODES.oneOf() nondet c=NODES.oneOf() nondet g=GROUPS.oneOf() replicate(n,c,g) },
        { nondet n=NODES.oneOf() nondet g=GROUPS.oneOf() nondet t=1.to(10).oneOf() step_down(n,g,t) },
        { nondet a=NODES.oneOf() nondet b=NODES.oneOf() partition(a,b) }, heal,
        { nondet a=NODES.oneOf() nondet b=NODES.oneOf() nondet g=GROUPS.oneOf() queue_snapshot(a,b,g) },
        { nondet message=snapshots.oneOf() receive_snapshot(message) },
        { nondet n=NODES.oneOf() crash(n) }, { nondet n=NODES.oneOf() recover(n) },
    }
    // Abstraction maps historical certificates to an abstract leader relation.
    // Safety relation: each concrete certificate names at most one abstract leader.
    pure def abstract_leaders(certificates:(GroupId,Term) -> Set[NodeId]):Set[(GroupId,Term,NodeId)] =
        certificates.keys().map(((g,t)) => certificates.get((g,t)).map(n => (g,t,n))).flatten()
    val concrete_abstract_safety:bool = abstract_leaders(elected).forall(a => val (ag,at,an) = a
        abstract_leaders(elected).forall(b => val (bg,bt,bn) = b
            (ag == bg and at == bt) implies an == bn))
    val election_safety: bool = elected.keys().forall(k => elected.get(k).size() <= 1) and
        GROUPS.forall(g => NODES.forall(a => NODES.forall(b =>
            (group_state.get((a,g)).role == "leader" and group_state.get((b,g)).role == "leader" and
             group_state.get((a,g)).current_term == group_state.get((b,g)).current_term) implies a == b)))
    val log_matching: bool = GROUPS.forall(g => NODES.forall(a => NODES.forall(b =>
        val l=group_state.get((a,g)).log
        val r=group_state.get((b,g)).log
        val common_len=if (l.length()<r.length()) l.length() else r.length()
        0.to(common_len-1).forall(i => l[i].term == r[i].term implies
            0.to(i).forall(j => l[j] == r[j])))))

    action unchanged = all { group_state' = group_state, snapshots' = snapshots, responses' = responses, requests' = requests, votes' = votes, elected' = elected, alive' = alive, partitioned' = partitioned }
}
