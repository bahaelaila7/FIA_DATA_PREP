"""nimiwae_jax — the D70 joint NIMIWAE age model in JAX / Flax NNX / optax (DECISIONS.md D75).

What it covers: the configuration the cross-validation trains (FIA's known design rule, flexible growth, no process noise, static prior),
age on or off, the Gaussian or the flow proposal (D73), and the D74 terms (measured-age anchors, the age-vigour coupling, Student-t height).

How it fits with the PyTorch code:
  * The data build stays in PyTorch / NumPy (nimiwae70.build): it is preprocessing, not training, and both backends must read the same tensors.
  * Training runs here: `train.train` has the signature of `nimiwae70.train` and returns a PyTorch `NIMIWAE70` with the trained weights.
  * Scoring, the cross-validation hook and the flow check keep using the PyTorch module: `weights.to_torch` copies the Flax parameters into it.
  * `equivalence.check` loads one set of weights into both frameworks, feeds the same noise and compares log weights, DReG weights and both
    gradient sets, so the port is tested against the code it replaces rather than against itself.

Modules: flow.py (rational-quadratic spline coupling flow), model.py (encoder and generative subtrees, the joint log weights),
weights.py (PyTorch <-> Flax parameters), train.py (the DReG step and the training loop), equivalence.py (the check against PyTorch)."""
