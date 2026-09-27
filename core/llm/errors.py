"""Safe, provider-independent failures for generated output."""


class LLMOutputTruncatedError(ValueError):
    """The provider stopped at its output limit; partial output is unusable."""

