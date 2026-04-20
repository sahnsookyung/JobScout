"""FastAPI router exposing the Prometheus text-format scrape endpoint."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

# Import for side effect: registers every singleton Counter/Histogram in
# ``core.metrics`` so ``/metrics`` returns ``# HELP`` / ``# TYPE`` lines for
# all of them even before a single emit site fires.
from core import metrics as _metrics_declarations  # noqa: F401

router = APIRouter()


@router.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
