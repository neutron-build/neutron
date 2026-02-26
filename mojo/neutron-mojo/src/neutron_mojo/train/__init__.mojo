# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Training Package
# ===----------------------------------------------------------------------=== #

"""Training utilities: modules, losses, training loop, trainable transformer, E2E."""

from .modules import Linear, Embedding, RMSNormModule, LayerNormModule, Dropout
from .losses import (
    log_softmax, cross_entropy_loss, mse_loss, l1_loss,
    binary_cross_entropy, kl_divergence, sequence_cross_entropy_loss,
)
from .loop import TrainingConfig, TrainingState, TrainingMetrics, estimate_training_memory
from .trainable import TrainableTransformerBlock, TrainableLM, causal_lm_loss
from .weight_transfer import WeightMapping, build_weight_mapping, model_to_tape, tape_to_model
from .lora_train import TrainableLoRA, LoRATrainableLM
# NOTE: e2e is imported directly (not via __init__) to avoid circular dependency:
#   train/__init__ -> .e2e -> neutron_mojo.train.trainable -> train/__init__
# Use: from neutron_mojo.train.e2e import train_tiny_lm, create_simple_dataset, TrainResult
