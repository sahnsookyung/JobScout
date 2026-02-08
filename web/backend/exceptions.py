#!/usr/bin/env python3
"""
Custom exceptions and error handlers for the web application.
"""

import logging
from typing import Any, Dict
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class ServiceException(Exception):
    """Base exception for service layer errors."""
    pass


class MatchNotFoundException(ServiceException):
    """Raised when a match is not found."""
    pass


class JobNotFoundException(ServiceException):
    """Raised when a job is not found."""
    pass


class PipelineLockedException(ServiceException):
    """Raised when pipeline is already running."""
    pass


class InvalidPolicyException(ServiceException):
    """Raised when policy configuration is invalid."""
    pass


class NotificationException(ServiceException):
    """Raised when notification fails."""
    pass


async def service_exception_handler(
    request: Request,
    exc: ServiceException
) -> JSONResponse:
    """
    Handle service layer exceptions.
    
    Args:
        request: The FastAPI request.
        exc: The service exception.
    
    Returns:
        JSONResponse with error details.
    """
    logger.error(f"Service error in {request.url.path}: {exc}", exc_info=True)
    
    status_code = 500
    if isinstance(exc, (MatchNotFoundException, JobNotFoundException)):
        status_code = 404
    elif isinstance(exc, (InvalidPolicyException, PipelineLockedException)):
        status_code = 400
    
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "error": str(exc),
            "type": exc.__class__.__name__
        }
    )


async def http_exception_handler(
    request: Request,
    exc: HTTPException
) -> JSONResponse:
    """
    Handle FastAPI HTTP exceptions with consistent format.
    
    Args:
        request: The FastAPI request.
        exc: The HTTP exception.
    
    Returns:
        JSONResponse with error details.
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "success": False,
            "error": exc.detail,
            "type": "HTTPException"
        }
    )


async def general_exception_handler(
    request: Request,
    exc: Exception
) -> JSONResponse:
    """
    Handle unexpected exceptions.
    
    Args:
        request: The FastAPI request.
        exc: The exception.
    
    Returns:
        JSONResponse with error details.
    """
    logger.exception(f"Unexpected error in {request.url.path}")
    
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": "Internal server error",
            "type": "InternalError"
        }
    )
