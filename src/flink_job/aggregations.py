import math
import statistics
from typing import List


def compute_stats(values: List[float]) -> dict:
    if not values:
        return {}

    return {
        "min": min(values),
        "max": max(values),
        "median": statistics.median(values),
        "mean": statistics.mean(values),
        "std_dev": statistics.pstdev(values) if len(values) > 1 else 0.0
    }
