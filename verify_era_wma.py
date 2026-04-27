from batch.pipeline.score_game import _compute_confidence_score
import inspect
src = inspect.getsource(_compute_confidence_score)
print("era_wma in scorer:    ", "era_wma" in src)
print("era_src_label in scorer:", "era_src_label" in src)
