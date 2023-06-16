from typing import Optional

from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
    AssertionType,
)


class AssertionEvaluator:
    """Base class for all assertion evaluators."""

    @property
    def type(self) -> AssertionType:
        raise NotImplementedError()

    def evaluate(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        raise NotImplementedError()
