from abc import ABC, abstractmethod
from typing import Optional

from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
)


class AssertionResultHandler(ABC):
    """Base class for all assertion result handlers."""

    @abstractmethod
    def handle(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        result: AssertionEvaluationResult,
        context: AssertionEvaluationContext,
    ) -> None:
        raise NotImplementedError()
