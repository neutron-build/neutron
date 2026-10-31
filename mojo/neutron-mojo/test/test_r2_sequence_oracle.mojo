"""Two-token transformer oracle: independent scalar forward and every parameter FD."""
from std.math import sqrt, exp, abs
from std.testing import assert_true, assert_equal
from neutron_mojo.autograd.tape import Tape
from neutron_mojo.autograd.ops import tracked_sum
from neutron_mojo.autograd.backward import run_backward
from neutron_mojo.train.trainable import TrainableLM


def norm_reference(tape: Tape, x: List[Float64], gamma: Int) -> List[Float64]:
    var rms = sqrt((x[0] * x[0] + x[1] * x[1]) / 2 + 1e-6)
    var out = List[Float64]()
    out.append(x[0] / rms * Float64(tape.get_data(gamma, 0)))
    out.append(x[1] / rms * Float64(tape.get_data(gamma, 1)))
    return out^


def linear_reference(tape: Tape, x: List[Float64], weight: Int, rows: Int = 2) -> List[Float64]:
    var result = List[Float64]()
    for row in range(rows):
        result.append(Float64(tape.get_data(weight, row * 2)) * x[0] + Float64(tape.get_data(weight, row * 2 + 1)) * x[1])
    return result^


def reference(tape: Tape, model: TrainableLM, pos: Int) -> List[Float64]:
    var keys = List[Float64]()
    var vals = List[Float64]()
    var query = List[Float64]()
    query.append(Float64(0))
    query.append(Float64(0))
    var residual = List[Float64]()
    residual.append(Float64(0))
    residual.append(Float64(0))
    for token in range(pos + 1):
        var x = List[Float64]()
        x.append(Float64(tape.get_data(model.embedding.embed_idx, token * 2)))
        x.append(Float64(tape.get_data(model.embedding.embed_idx, token * 2 + 1)))
        var normalized = norm_reference(tape, x, model.blocks[0].attn_norm.gamma_idx)
        var key = linear_reference(tape, normalized, model.blocks[0].k_proj.weight_idx)
        var val = linear_reference(tape, normalized, model.blocks[0].v_proj.weight_idx)
        for d in range(2):
            keys.append(key[d])
            vals.append(val[d])
        if token == pos:
            query = linear_reference(tape, normalized, model.blocks[0].q_proj.weight_idx)
            residual = x^
    var scores = List[Float64]()
    var maximum = Float64(-1e100)
    for token in range(pos + 1):
        var score = (query[0] * keys[token * 2] + query[1] * keys[token * 2 + 1]) / sqrt(Float64(2))
        scores.append(score)
        maximum = max(maximum, score)
    var denominator = Float64(0)
    for token in range(pos + 1):
        denominator += exp(scores[token] - maximum)
    var context = List[Float64]()
    context.append(Float64(0))
    context.append(Float64(0))
    for token in range(pos + 1):
        var probability = exp(scores[token] - maximum) / denominator
        for d in range(2):
            context[d] += probability * vals[token * 2 + d]
    var projected = linear_reference(tape, context, model.blocks[0].o_proj.weight_idx)
    for d in range(2):
        residual[d] += projected[d]
    var normalized = norm_reference(tape, residual, model.blocks[0].ffn_norm.gamma_idx)
    var gate = linear_reference(tape, normalized, model.blocks[0].gate_proj.weight_idx)
    var up = linear_reference(tape, normalized, model.blocks[0].up_proj.weight_idx)
    var hidden = List[Float64]()
    hidden.append(max(gate[0], Float64(0)) * up[0])
    hidden.append(max(gate[1], Float64(0)) * up[1])
    var down = linear_reference(tape, hidden, model.blocks[0].down_proj.weight_idx)
    for d in range(2):
        residual[d] += down[d]
    var final = norm_reference(tape, residual, model.final_norm.gamma_idx)
    return linear_reference(tape, final, model.lm_head.weight_idx, 3)


def objective(tape: Tape, model: TrainableLM) -> Float64:
    var logits = reference(tape, model, 1)
    return logits[0] + logits[1] + logits[2]


def assign(mut tape: Tape, idx: Int, *values: Float64):
    for i in range(len(values)):
        tape.set_data(idx, i, Float32(values[i]))


def main() raises:
    var tape = Tape(2048)
    var model = TrainableLM(3, 2, 1, 2)
    model.register(tape)
    assign(tape, model.embedding.embed_idx, [1,-0.5,-0.7,1.2,0.3,0.9])
    assign(tape, model.blocks[0].attn_norm.gamma_idx, [1.3,0.8])
    assign(tape, model.blocks[0].ffn_norm.gamma_idx, [0.9,1.1])
    assign(tape, model.final_norm.gamma_idx, [1.2,0.7])
    assign(tape, model.blocks[0].q_proj.weight_idx, [0.4,-0.2,0.3,0.7])
    assign(tape, model.blocks[0].k_proj.weight_idx, [-0.1,0.6,0.8,0.2])
    assign(tape, model.blocks[0].v_proj.weight_idx, [0.7,0.3,-0.4,0.5])
    assign(tape, model.blocks[0].o_proj.weight_idx, [0.4,-0.3,0.2,0.6])
    assign(tape, model.blocks[0].gate_proj.weight_idx, [0.4,0.1,0.2,-0.3])
    assign(tape, model.blocks[0].up_proj.weight_idx, [0.3,0.2,-0.1,0.5])
    assign(tape, model.blocks[0].down_proj.weight_idx, [0.2,-0.4,0.6,0.1])
    assign(tape, model.lm_head.weight_idx, [0.4,-0.2,-0.3,0.6,0.2,0.1])
    var frontier = tape.num_variables()
    var params = model.all_param_indices()
    var tokens = List[Int]()
    tokens.append(0)
    tokens.append(1)
    var logits = model.forward_seq(tape, tokens)
    var first = List[Float32]()
    for pos in range(2):
        var expected = reference(tape, model, pos)
        for d in range(3):
            assert_true(abs(Float64(tape.get_data(logits[pos], d)) - expected[d]) < 0.00002)
            if pos == 0:
                first.append(tape.get_data(logits[0], d))
    var loss = tracked_sum(tape, logits[1])
    run_backward(tape, loss)
    var q_signal = Float64(0)
    var k_signal = Float64(0)
    for p in range(len(params)):
        var idx = params[p]
        for i in range(tape.var_numel(idx)):
            var original = tape.get_data(idx, i)
            var upper = original + Float32(0.001)
            var lower = original - Float32(0.001)
            tape.set_data(idx, i, upper)
            var plus = objective(tape, model)
            tape.set_data(idx, i, lower)
            var minus = objective(tape, model)
            tape.set_data(idx, i, original)
            var expected = (plus - minus) / Float64(upper - lower)
            var actual = Float64(tape.get_grad(idx, i))
            assert_true(abs(actual - expected) < 0.002 + 0.003 * abs(expected))
            if idx == model.blocks[0].q_proj.weight_idx:
                q_signal += abs(actual)
            if idx == model.blocks[0].k_proj.weight_idx:
                k_signal += abs(actual)
    assert_true(q_signal > 0.0001 and k_signal > 0.0001)
    tape.rewind_activations(frontier)
    # Future token changes must not affect position zero.
    tape.set_data(model.embedding.embed_idx, 2, -2)
    tape.set_data(model.embedding.embed_idx, 3, 3)
    var changed = model.forward_seq(tape, tokens)
    for d in range(3):
        assert_equal(tape.get_data(changed[0], d), first[d])
    print("PASS: r2 two-token independent forward/every-parameter/causality oracle")
