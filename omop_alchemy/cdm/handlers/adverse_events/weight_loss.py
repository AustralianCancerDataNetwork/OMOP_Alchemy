from __future__ import annotations

from typing import ClassVar, Optional

from .ctcae import ctcae_weight_loss_grade


class MartinWeightLoss:
    """
    Martin et al. BMI-adjusted percent-weight-loss grading.

    This matrix is kept in adverse events because it is clinical 
    severity policy over body measurements, not body-measurement 
    arithmetic itself.
    """

    BMI_CATEGORY_BOUNDARIES: ClassVar[tuple[float, ...]] = (20.0, 22.0, 25.0, 28.0)
    WEIGHT_LOSS_CATEGORY_BOUNDARIES: ClassVar[tuple[float, ...]] = (
        2.5,
        6.0,
        11.0,
        15.0,
    )
    GRADE_MATRIX: ClassVar[tuple[tuple[int, int, int, int, int], ...]] = (
        (0, 0, 1, 1, 3),
        (1, 2, 2, 2, 3),
        (2, 3, 3, 3, 4),
        (3, 3, 4, 4, 4),
        (3, 4, 4, 4, 4),
    )

    @classmethod
    def _bmi_category_index(cls, bmi: float) -> int:
        for index, boundary in enumerate(reversed(cls.BMI_CATEGORY_BOUNDARIES)):
            if bmi >= boundary:
                return index
        return len(cls.BMI_CATEGORY_BOUNDARIES)

    @classmethod
    def _weight_loss_category_index(cls, loss_pct: float) -> int:
        for index, boundary in enumerate(cls.WEIGHT_LOSS_CATEGORY_BOUNDARIES):
            if loss_pct < boundary:
                return index
        return len(cls.WEIGHT_LOSS_CATEGORY_BOUNDARIES)

    @classmethod
    def grade(
        cls,
        pct_change: Optional[float],
        bmi: Optional[float],
    ) -> Optional[int]:
        if pct_change is None or bmi is None:
            return None
        loss_pct = max(0.0, -pct_change)
        return cls.GRADE_MATRIX[
            cls._weight_loss_category_index(loss_pct)
        ][cls._bmi_category_index(bmi)]


def martin_weight_loss_grade(
    pct_change: Optional[float],
    bmi: Optional[float],
) -> Optional[int]:
    return MartinWeightLoss.grade(pct_change, bmi)


def critical_weight_loss_grade(
    pct_change: Optional[float],
    bmi: Optional[float],
) -> Optional[int]:
    """
    Critical-weight-loss grade using Martin where BMI is available.

    Falls back to CTCAE-style percent-weight-loss grading when percent change is
    evaluable but BMI is not, preserving coverage without guessing BMI.
    """
    grade = martin_weight_loss_grade(pct_change, bmi)
    return grade if grade is not None else ctcae_weight_loss_grade(pct_change)
