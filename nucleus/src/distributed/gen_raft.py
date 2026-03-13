# Generates raft_message_block.rs
import sys
out_path = sys.argv[1]
L = []
def a(s): L.append(s)
a("// --- RaftMessage (wire-format for inter-node Raft communication) ---")
a("")
a("/// Messages exchanged between Raft nodes.")
a("#[derive(Debug, Clone, PartialEq)]")
a("pub enum RaftMessage {")
# ... this is going to be way too long line by line
# Use a different approach: read from base64
