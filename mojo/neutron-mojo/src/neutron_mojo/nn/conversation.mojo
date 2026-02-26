# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Multi-Turn Conversation Manager
# ===----------------------------------------------------------------------=== #

"""Session state and message history for multi-turn conversations.

Manages chat sessions with role-tagged messages, multi-turn template
formatting, and integration with the generation pipeline. Supports
both Llama and ChatML templates with full conversation history.

Usage:
    var session = ConversationSession("sess-1", "You are a helpful assistant.")
    session.add_user_message("What is 2+2?")
    var reply = conversation_generate(model, tokenizer, session, config)
    session.add_assistant_message(reply)
    session.add_user_message("And 3+3?")
    var reply2 = conversation_generate(model, tokenizer, session, config)
"""

from neutron_mojo.tensor.tensor import Tensor
from neutron_mojo.tensor.shape import Shape
from neutron_mojo.nn.model import Model, ModelParams
from neutron_mojo.nn.kv_cache import MultiLayerKVCache
from neutron_mojo.nn.q_kv_cache import MultiLayerQ8KVCache
from neutron_mojo.nn.rope import RoPETable
from neutron_mojo.nn.sampler import Sampler, SamplerConfig
from neutron_mojo.nn.generation import (
    apply_repetition_penalty,
    apply_frequency_penalty,
    should_stop,
)
from neutron_mojo.nn.tokenizer import BPETokenizer
from neutron_mojo.nn.pipeline import PipelineConfig


# ===----------------------------------------------------------------------=== #
# ChatMessage — A single message in a conversation
# ===----------------------------------------------------------------------=== #

struct ChatMessage(Copyable, Movable):
    """A single message in a conversation with a role tag.

    Roles: "system", "user", "assistant"
    """
    var role: String
    var content: String

    fn __init__(out self, role: String, content: String):
        self.role = role
        self.content = content

    fn __copyinit__(out self, existing: Self):
        self.role = existing.role
        self.content = existing.content

    fn __moveinit__(out self, deinit other: Self):
        self.role = other.role^
        self.content = other.content^

    fn copy(self) -> ChatMessage:
        """Return a copy of this message."""
        return ChatMessage(self.role, self.content)

    fn is_system(self) -> Bool:
        return self.role == "system"

    fn is_user(self) -> Bool:
        return self.role == "user"

    fn is_assistant(self) -> Bool:
        return self.role == "assistant"


# ===----------------------------------------------------------------------=== #
# ConversationSession — Multi-turn session state
# ===----------------------------------------------------------------------=== #

struct ConversationSession(Movable):
    """Manages message history and state for a multi-turn conversation.

    Stores all messages in order, tracks turn count, and provides
    template formatting for the full conversation history.
    """
    var session_id: String
    var system_prompt: String
    var messages: List[ChatMessage]
    var turn_count: Int          # Number of user-assistant exchanges
    var max_history: Int         # Max messages to keep (0 = unlimited)

    fn __init__(out self, session_id: String, system_prompt: String = ""):
        """Create a new conversation session.

        Args:
            session_id: Unique session identifier.
            system_prompt: Optional system prompt prepended to conversation.
        """
        self.session_id = session_id
        self.system_prompt = system_prompt
        self.messages = List[ChatMessage]()
        self.turn_count = 0
        self.max_history = 0  # unlimited

    fn __init__(out self, session_id: String, system_prompt: String,
                max_history: Int):
        self.session_id = session_id
        self.system_prompt = system_prompt
        self.messages = List[ChatMessage]()
        self.turn_count = 0
        self.max_history = max_history

    fn __moveinit__(out self, deinit other: Self):
        self.session_id = other.session_id^
        self.system_prompt = other.system_prompt^
        self.messages = other.messages^
        self.turn_count = other.turn_count
        self.max_history = other.max_history

    fn add_message(mut self, role: String, content: String):
        """Add a message to the conversation history.

        Args:
            role: Message role ("system", "user", "assistant").
            content: Message text content.
        """
        self.messages.append(ChatMessage(role, content))
        if role == "assistant":
            self.turn_count += 1
        self._enforce_history_limit()

    fn add_user_message(mut self, content: String):
        """Add a user message."""
        self.add_message("user", content)

    fn add_assistant_message(mut self, content: String):
        """Add an assistant response."""
        self.add_message("assistant", content)

    fn add_system_message(mut self, content: String):
        """Add a system message."""
        self.add_message("system", content)

    fn num_messages(self) -> Int:
        """Total number of messages in history."""
        return len(self.messages)

    fn get_last_user_message(self) -> String:
        """Get the most recent user message content.

        Returns:
            Last user message, or empty string if none.
        """
        var i = len(self.messages) - 1
        while i >= 0:
            if self.messages[i].role == "user":
                return String(self.messages[i].content)
            i -= 1
        return String("")

    fn get_last_assistant_message(self) -> String:
        """Get the most recent assistant message content.

        Returns:
            Last assistant message, or empty string if none.
        """
        var i = len(self.messages) - 1
        while i >= 0:
            if self.messages[i].role == "assistant":
                return String(self.messages[i].content)
            i -= 1
        return String("")

    fn clear(mut self):
        """Clear all messages and reset turn count."""
        self.messages = List[ChatMessage]()
        self.turn_count = 0

    fn _enforce_history_limit(mut self):
        """Trim old messages if max_history is set.

        Keeps system messages and trims oldest user/assistant pairs
        to stay within the limit.
        """
        if self.max_history <= 0 or len(self.messages) <= self.max_history:
            return

        # Keep the most recent max_history messages
        var new_messages = List[ChatMessage]()
        var start = len(self.messages) - self.max_history
        if start < 0:
            start = 0
        for i in range(start, len(self.messages)):
            new_messages.append(self.messages[i].copy())
        self.messages = new_messages^


# ===----------------------------------------------------------------------=== #
# Multi-Turn Template Formatting
# ===----------------------------------------------------------------------=== #

fn format_conversation_llama(session: ConversationSession) -> String:
    """Format full conversation history using Llama instruct template.

    Format:
        <<SYS>>
        system prompt
        <</SYS>>

        [INST] user message 1 [/INST] assistant reply 1
        [INST] user message 2 [/INST] assistant reply 2
        [INST] latest user message [/INST]

    Args:
        session: Conversation session with message history.

    Returns:
        Formatted conversation string.
    """
    var result = String("")

    # System prompt
    if len(session.system_prompt) > 0:
        result += "<<SYS>>\n" + session.system_prompt + "\n<</SYS>>\n\n"

    # Messages
    for i in range(len(session.messages)):
        var msg = session.messages[i].copy()
        if msg.role == "system":
            # System messages mid-conversation: inline
            result += "<<SYS>>\n" + msg.content + "\n<</SYS>>\n\n"
        elif msg.role == "user":
            result += "[INST] " + msg.content + " [/INST]"
            # Check if next message is assistant reply
            var has_reply = False
            if i + 1 < len(session.messages):
                if session.messages[i + 1].copy().role == "assistant":
                    has_reply = True
            if not has_reply:
                # Last user message — no closing, model generates from here
                pass
        elif msg.role == "assistant":
            result += " " + msg.content + "\n"

    return result^


fn format_conversation_chatml(session: ConversationSession) -> String:
    """Format full conversation history using ChatML template.

    Format:
        <|im_start|>system
        system prompt<|im_end|>
        <|im_start|>user
        message<|im_end|>
        <|im_start|>assistant
        reply<|im_end|>
        <|im_start|>user
        latest message<|im_end|>
        <|im_start|>assistant

    Args:
        session: Conversation session with message history.

    Returns:
        Formatted conversation string.
    """
    var result = String("")

    # System prompt
    if len(session.system_prompt) > 0:
        result += "<|im_start|>system\n" + session.system_prompt + "<|im_end|>\n"

    # Messages
    for i in range(len(session.messages)):
        var msg = session.messages[i].copy()
        result += "<|im_start|>" + msg.role + "\n" + msg.content + "<|im_end|>\n"

    # Open assistant turn for generation
    result += "<|im_start|>assistant\n"

    return result^


fn format_conversation(session: ConversationSession, template: String) -> String:
    """Format conversation using the specified template.

    Args:
        session: Conversation session.
        template: Template name ("llama", "chatml", or "none").

    Returns:
        Formatted conversation string.
    """
    if template == "llama":
        return format_conversation_llama(session)
    elif template == "chatml":
        return format_conversation_chatml(session)

    # "none" — just concatenate messages with role prefixes
    var result = String("")
    if len(session.system_prompt) > 0:
        result += "System: " + session.system_prompt + "\n"
    for i in range(len(session.messages)):
        var msg = session.messages[i].copy()
        if msg.role == "user":
            result += "User: " + msg.content + "\n"
        elif msg.role == "assistant":
            result += "Assistant: " + msg.content + "\n"
        elif msg.role == "system":
            result += "System: " + msg.content + "\n"
    result += "Assistant: "
    return result^


# ===----------------------------------------------------------------------=== #
# Conversation Generate — Multi-turn generation with session
# ===----------------------------------------------------------------------=== #

fn conversation_generate(
    model: Model,
    tokenizer: BPETokenizer,
    session: ConversationSession,
    config: PipelineConfig,
) raises -> String:
    """Generate a response given the full conversation history.

    Formats the entire conversation (system prompt + all messages) into
    the appropriate template, encodes it, and runs the generation loop.
    The caller should add the returned text as an assistant message
    to the session.

    Args:
        model: The language model.
        tokenizer: BPE tokenizer.
        session: Conversation session with message history.
        config: Pipeline configuration (template, sampling, penalties).

    Returns:
        Generated assistant response text.
    """
    var p = model.params.copy()

    # 1. Format full conversation history
    var formatted = format_conversation(session, config.chat_template)

    # 2. Encode
    var input_ids = tokenizer.encode_with_special(formatted, add_bos=config.add_bos)

    # 3. Create infrastructure
    var total_len = len(input_ids) + config.max_new_tokens
    var rope = RoPETable(
        head_dim=p.head_dim,
        max_seq_len=total_len,
        theta=p.rope_theta,
    )
    var sampler = Sampler(config.sampler_config)

    # 4. Create cache and prefill
    var logits = Tensor[DType.float32](Shape(p.vocab_size))

    if config.use_q8_cache:
        var q8cache = MultiLayerQ8KVCache(
            num_layers=p.num_layers, max_seq_len=total_len,
            num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
        )
        for i in range(len(input_ids)):
            logits = model.forward_q8cache(input_ids[i], q8cache, rope, pos=i)

        # 5. Decode loop
        var generated = List[Int]()
        var stop_tokens = List[Int]()
        if tokenizer.eos_id >= 0:
            stop_tokens.append(tokenizer.eos_id)

        for step in range(config.max_new_tokens):
            if config.repetition_penalty > 1.0:
                apply_repetition_penalty(logits, p.vocab_size, generated, config.repetition_penalty)
            if config.frequency_penalty != 0.0 or config.presence_penalty != 0.0:
                apply_frequency_penalty(logits, p.vocab_size, generated, config.frequency_penalty, config.presence_penalty)
            var next_token = sampler.sample(logits, p.vocab_size)
            if should_stop(next_token, stop_tokens):
                break
            generated.append(next_token)
            var pos = len(input_ids) + step
            logits = model.forward_q8cache(next_token, q8cache, rope, pos=pos)

        return tokenizer.decode(generated)
    else:
        var cache = MultiLayerKVCache(
            num_layers=p.num_layers, max_seq_len=total_len,
            num_kv_heads=p.num_kv_heads, head_dim=p.head_dim,
        )
        for i in range(len(input_ids)):
            logits = model.forward(input_ids[i], cache, rope, pos=i)

        # 5. Decode loop
        var generated = List[Int]()
        var stop_tokens = List[Int]()
        if tokenizer.eos_id >= 0:
            stop_tokens.append(tokenizer.eos_id)

        for step in range(config.max_new_tokens):
            if config.repetition_penalty > 1.0:
                apply_repetition_penalty(logits, p.vocab_size, generated, config.repetition_penalty)
            if config.frequency_penalty != 0.0 or config.presence_penalty != 0.0:
                apply_frequency_penalty(logits, p.vocab_size, generated, config.frequency_penalty, config.presence_penalty)
            var next_token = sampler.sample(logits, p.vocab_size)
            if should_stop(next_token, stop_tokens):
                break
            generated.append(next_token)
            var pos = len(input_ids) + step
            logits = model.forward(next_token, cache, rope, pos=pos)

        return tokenizer.decode(generated)
