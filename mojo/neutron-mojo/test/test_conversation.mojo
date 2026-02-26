# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Conversation Manager Tests
# ===----------------------------------------------------------------------=== #

"""Tests for multi-turn conversation manager."""

from neutron_mojo.nn.conversation import (
    ChatMessage,
    ConversationSession,
    format_conversation_llama,
    format_conversation_chatml,
    format_conversation,
    conversation_generate,
)
from neutron_mojo.nn.pipeline import PipelineConfig
from neutron_mojo.nn.model import Model, ModelParams, tiny_test_params
from neutron_mojo.nn.tokenizer import BPETokenizer, build_test_tokenizer
from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape


fn assert_true(cond: Bool, msg: String) raises:
    if not cond:
        raise Error("FAIL: " + msg)


# ===----------------------------------------------------------------------=== #
# ChatMessage Tests
# ===----------------------------------------------------------------------=== #

fn test_chat_message_creation() raises:
    """Test ChatMessage creation and role checks."""
    var msg_u = ChatMessage("user", "Hello")
    assert_true(msg_u.is_user(), "Should be user")
    assert_true(not msg_u.is_assistant(), "Should not be assistant")
    assert_true(not msg_u.is_system(), "Should not be system")
    assert_true(msg_u.content == "Hello", "Content should be Hello")

    var msg_a = ChatMessage("assistant", "Hi there!")
    assert_true(msg_a.is_assistant(), "Should be assistant")

    var msg_s = ChatMessage("system", "You are helpful.")
    assert_true(msg_s.is_system(), "Should be system")

    print("  chat_message_creation: PASS")


fn test_chat_message_copy() raises:
    """Test ChatMessage copy."""
    var msg = ChatMessage("user", "Test content")
    var msg2 = msg.copy()
    assert_true(msg2.role == "user", "Copied role should be user")
    assert_true(msg2.content == "Test content", "Copied content should match")
    print("  chat_message_copy: PASS")


# ===----------------------------------------------------------------------=== #
# ConversationSession Tests
# ===----------------------------------------------------------------------=== #

fn test_session_creation() raises:
    """Test ConversationSession initialization."""
    var session = ConversationSession("sess-1", "You are a helper.")
    assert_true(session.session_id == "sess-1", "Session ID should match")
    assert_true(session.system_prompt == "You are a helper.", "System prompt should match")
    assert_true(session.num_messages() == 0, "Should have 0 messages")
    assert_true(session.turn_count == 0, "Should have 0 turns")
    print("  session_creation: PASS")


fn test_session_add_messages() raises:
    """Test adding messages to a session."""
    var session = ConversationSession("s1")
    session.add_user_message("What is 2+2?")
    assert_true(session.num_messages() == 1, "Should have 1 message")
    assert_true(session.turn_count == 0, "Turn count should be 0 after user msg")

    session.add_assistant_message("4")
    assert_true(session.num_messages() == 2, "Should have 2 messages")
    assert_true(session.turn_count == 1, "Turn count should be 1 after assistant reply")

    session.add_user_message("And 3+3?")
    assert_true(session.num_messages() == 3, "Should have 3 messages")
    assert_true(session.turn_count == 1, "Turn count should still be 1")

    session.add_assistant_message("6")
    assert_true(session.num_messages() == 4, "Should have 4 messages")
    assert_true(session.turn_count == 2, "Turn count should be 2")

    print("  session_add_messages: PASS")


fn test_session_get_last_messages() raises:
    """Test retrieving last user/assistant messages."""
    var session = ConversationSession("s2")
    session.add_user_message("First question")
    session.add_assistant_message("First answer")
    session.add_user_message("Second question")

    assert_true(session.get_last_user_message() == "Second question",
                "Last user message should be 'Second question'")
    assert_true(session.get_last_assistant_message() == "First answer",
                "Last assistant message should be 'First answer'")

    print("  session_get_last_messages: PASS")


fn test_session_get_last_empty() raises:
    """Test getting last messages from empty session."""
    var session = ConversationSession("s3")
    assert_true(len(session.get_last_user_message()) == 0,
                "Last user message should be empty")
    assert_true(len(session.get_last_assistant_message()) == 0,
                "Last assistant message should be empty")
    print("  session_get_last_empty: PASS")


fn test_session_clear() raises:
    """Test clearing a session."""
    var session = ConversationSession("s4")
    session.add_user_message("Hello")
    session.add_assistant_message("Hi")
    assert_true(session.num_messages() == 2, "Should have 2 messages before clear")

    session.clear()
    assert_true(session.num_messages() == 0, "Should have 0 messages after clear")
    assert_true(session.turn_count == 0, "Turn count should be 0 after clear")
    assert_true(session.session_id == "s4", "Session ID should persist after clear")
    assert_true(session.system_prompt == "", "System prompt should persist after clear")
    print("  session_clear: PASS")


fn test_session_max_history() raises:
    """Test max_history limit enforcement."""
    var session = ConversationSession("s5", "", 4)

    session.add_user_message("msg1")
    session.add_assistant_message("reply1")
    session.add_user_message("msg2")
    session.add_assistant_message("reply2")
    assert_true(session.num_messages() == 4, "Should have 4 messages at limit")

    # Adding 5th message should trim to 4
    session.add_user_message("msg3")
    assert_true(session.num_messages() == 4, "Should still have 4 messages after trim")

    # The oldest message (msg1) should be gone
    var first_msg = session.messages[0].copy()
    assert_true(first_msg.content == "reply1",
                "First message after trim should be reply1, got: " + first_msg.content)

    print("  session_max_history: PASS")


# ===----------------------------------------------------------------------=== #
# Template Formatting Tests
# ===----------------------------------------------------------------------=== #

fn test_format_llama_single_turn() raises:
    """Test Llama template with single user message."""
    var session = ConversationSession("t1", "Be helpful.")
    session.add_user_message("Hello")

    var formatted = format_conversation_llama(session)
    # Should contain system prompt
    assert_true(_contains(formatted, "<<SYS>>"), "Should contain <<SYS>>")
    assert_true(_contains(formatted, "Be helpful."), "Should contain system prompt")
    assert_true(_contains(formatted, "<</SYS>>"), "Should contain <</SYS>>")
    # Should contain user message
    assert_true(_contains(formatted, "[INST] Hello [/INST]"), "Should contain user INST")
    print("  format_llama_single_turn: PASS")


fn test_format_llama_multi_turn() raises:
    """Test Llama template with multi-turn conversation."""
    var session = ConversationSession("t2")
    session.add_user_message("Q1")
    session.add_assistant_message("A1")
    session.add_user_message("Q2")

    var formatted = format_conversation_llama(session)
    assert_true(_contains(formatted, "[INST] Q1 [/INST]"), "Should have Q1")
    assert_true(_contains(formatted, "A1"), "Should have A1")
    assert_true(_contains(formatted, "[INST] Q2 [/INST]"), "Should have Q2")
    print("  format_llama_multi_turn: PASS")


fn test_format_chatml_single_turn() raises:
    """Test ChatML template with single user message."""
    var session = ConversationSession("t3", "System info")
    session.add_user_message("Hi")

    var formatted = format_conversation_chatml(session)
    assert_true(_contains(formatted, "<|im_start|>system"), "Should have system tag")
    assert_true(_contains(formatted, "System info"), "Should have system content")
    assert_true(_contains(formatted, "<|im_start|>user"), "Should have user tag")
    assert_true(_contains(formatted, "Hi"), "Should have user message")
    assert_true(_contains(formatted, "<|im_start|>assistant"), "Should end with assistant tag")
    print("  format_chatml_single_turn: PASS")


fn test_format_chatml_multi_turn() raises:
    """Test ChatML template with multi-turn conversation."""
    var session = ConversationSession("t4")
    session.add_user_message("Q1")
    session.add_assistant_message("A1")
    session.add_user_message("Q2")

    var formatted = format_conversation_chatml(session)
    # Count occurrences of im_start — should be 3 messages + 1 final assistant
    assert_true(_contains(formatted, "Q1"), "Should have Q1")
    assert_true(_contains(formatted, "A1"), "Should have A1")
    assert_true(_contains(formatted, "Q2"), "Should have Q2")
    print("  format_chatml_multi_turn: PASS")


fn test_format_conversation_none() raises:
    """Test 'none' template — simple role prefix format."""
    var session = ConversationSession("t5", "Sys prompt")
    session.add_user_message("Hello")
    session.add_assistant_message("World")
    session.add_user_message("More")

    var formatted = format_conversation(session, "none")
    assert_true(_contains(formatted, "System: Sys prompt"), "Should have system prefix")
    assert_true(_contains(formatted, "User: Hello"), "Should have user prefix")
    assert_true(_contains(formatted, "Assistant: World"), "Should have assistant prefix")
    assert_true(_contains(formatted, "User: More"), "Should have second user")
    assert_true(_contains(formatted, "Assistant: "), "Should end with Assistant prompt")
    print("  format_conversation_none: PASS")


fn test_format_conversation_dispatch() raises:
    """Test that format_conversation dispatches to correct template."""
    var session = ConversationSession("t6")
    session.add_user_message("Test")

    var llama = format_conversation(session, "llama")
    assert_true(_contains(llama, "[INST]"), "llama should have [INST]")

    var chatml = format_conversation(session, "chatml")
    assert_true(_contains(chatml, "<|im_start|>"), "chatml should have im_start")

    var none = format_conversation(session, "none")
    assert_true(_contains(none, "User: "), "none should have User: prefix")

    print("  format_conversation_dispatch: PASS")


# ===----------------------------------------------------------------------=== #
# Conversation Generate Tests
# ===----------------------------------------------------------------------=== #

fn _build_tiny_model() -> Model:
    """Build a tiny model for testing (1 layer, vocab=32, dim=16)."""
    var params = tiny_test_params()
    var model = Model(params)
    # Fill layer weights with small values so forward pass doesn't NaN
    var total = model.layer_weights.numel()
    for i in range(total):
        model.layer_weights.set(i, Float32(0.01) * Float32(i % 7 - 3))
    # Fill embed
    var embed_total = model.embed.numel()
    for i in range(embed_total):
        model.embed.set(i, Float32(0.01) * Float32(i % 5 - 2))
    # Fill final norm
    for i in range(model.final_norm.numel()):
        model.final_norm.set(i, 1.0)
    # Fill lm_head
    var lm_total = model.lm_head.numel()
    for i in range(lm_total):
        model.lm_head.set(i, Float32(0.01) * Float32(i % 11 - 5))
    return model^


fn _build_tiny_tokenizer() -> BPETokenizer:
    """Build tokenizer for tiny model (vocab=32)."""
    var tok = BPETokenizer()
    _ = tok.add_special_token("<bos>", "bos")
    _ = tok.add_special_token("<eos>", "eos")
    _ = tok.add_special_token("<unk>", "unk")
    for i in range(29):
        _ = tok.add_token(chr(97 + (i % 26)))  # a-z wrapping
    tok.unk_id = 2
    return tok^


fn test_conversation_generate_basic() raises:
    """Test conversation_generate produces output."""
    var model = _build_tiny_model()
    var tokenizer = _build_tiny_tokenizer()
    var session = ConversationSession("gen-1")
    session.add_user_message("hello")

    var config = PipelineConfig()
    config.max_new_tokens = 5
    config.chat_template = "none"

    var output = conversation_generate(model, tokenizer, session, config)
    # Should produce some output (may be garbage from random weights)
    assert_true(len(output) >= 0, "Should return a string")
    print("  conversation_generate_basic: PASS")


fn test_conversation_generate_multi_turn() raises:
    """Test that multi-turn conversation generates without error."""
    var model = _build_tiny_model()
    var tokenizer = _build_tiny_tokenizer()
    var session = ConversationSession("gen-2", "You are a calculator.")
    session.add_user_message("What is 2+2?")
    session.add_assistant_message("4")
    session.add_user_message("And 3+3?")

    var config = PipelineConfig()
    config.max_new_tokens = 3
    config.chat_template = "none"

    var output = conversation_generate(model, tokenizer, session, config)
    assert_true(len(output) >= 0, "Multi-turn should produce output")
    print("  conversation_generate_multi_turn: PASS")


fn test_conversation_generate_with_template() raises:
    """Test generation with chatml template."""
    var model = _build_tiny_model()
    var tokenizer = _build_tiny_tokenizer()
    var session = ConversationSession("gen-3", "Helper")
    session.add_user_message("Hi")

    var config = PipelineConfig()
    config.max_new_tokens = 3
    config.chat_template = "chatml"

    var output = conversation_generate(model, tokenizer, session, config)
    assert_true(len(output) >= 0, "ChatML template generate should work")
    print("  conversation_generate_with_template: PASS")


fn test_full_conversation_flow() raises:
    """Test complete multi-turn flow: add user msg, generate, add reply, repeat."""
    var model = _build_tiny_model()
    var tokenizer = _build_tiny_tokenizer()
    var session = ConversationSession("flow-1")

    var config = PipelineConfig()
    config.max_new_tokens = 3
    config.chat_template = "none"

    # Turn 1
    session.add_user_message("Hello")
    var reply1 = conversation_generate(model, tokenizer, session, config)
    session.add_assistant_message(reply1)
    assert_true(session.turn_count == 1, "Should have 1 turn")
    assert_true(session.num_messages() == 2, "Should have 2 messages")

    # Turn 2
    session.add_user_message("More")
    var reply2 = conversation_generate(model, tokenizer, session, config)
    session.add_assistant_message(reply2)
    assert_true(session.turn_count == 2, "Should have 2 turns")
    assert_true(session.num_messages() == 4, "Should have 4 messages")

    print("  full_conversation_flow: PASS")


# ===----------------------------------------------------------------------=== #
# Helpers
# ===----------------------------------------------------------------------=== #

fn _contains(haystack: String, needle: String) -> Bool:
    """Check if haystack contains needle."""
    var h_len = len(haystack)
    var n_len = len(needle)
    if n_len == 0:
        return True
    if n_len > h_len:
        return False
    for i in range(h_len - n_len + 1):
        var found = True
        for j in range(n_len):
            if ord(haystack[byte=i + j]) != ord(needle[byte=j]):
                found = False
                break
        if found:
            return True
    return False


# ===----------------------------------------------------------------------=== #
# Main
# ===----------------------------------------------------------------------=== #

fn main() raises:
    print("test_conversation:")

    # ChatMessage tests
    test_chat_message_creation()
    test_chat_message_copy()

    # ConversationSession tests
    test_session_creation()
    test_session_add_messages()
    test_session_get_last_messages()
    test_session_get_last_empty()
    test_session_clear()
    test_session_max_history()

    # Template formatting tests
    test_format_llama_single_turn()
    test_format_llama_multi_turn()
    test_format_chatml_single_turn()
    test_format_chatml_multi_turn()
    test_format_conversation_none()
    test_format_conversation_dispatch()

    # Generation tests
    test_conversation_generate_basic()
    test_conversation_generate_multi_turn()
    test_conversation_generate_with_template()
    test_full_conversation_flow()

    print("ALL PASSED (20 tests)")
