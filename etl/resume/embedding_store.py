#!/usr/bin/env python3
"""
Resume Embedding Store Protocol - Abstract interface for persisting resume embeddings.

This module defines the protocol that resume embedding persistence must implement.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class ResumeSectionEmbeddingStore(Protocol):
    """
    Protocol for storing resume section embeddings.
    """

    @abstractmethod
    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[Dict[str, Any]]
    ) -> None:
        """
        Save resume section embeddings.

        Args:
            resume_fingerprint: Unique identifier for the resume
            sections: List of section dictionaries with embedding data
        """
        pass

    @abstractmethod
    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> List[Any]:
        """
        Retrieve resume section embeddings.

        Args:
            resume_fingerprint: Unique identifier for the resume
            section_type: Optional filter by section type (e.g., 'experience', 'skills')

        Returns:
            List of section embedding records
        """
        pass


@runtime_checkable
class ResumeEvidenceUnitEmbeddingStore(Protocol):
    """
    Protocol for persisting resume evidence unit embeddings.
    """

    @abstractmethod
    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[Dict[str, Any]]
    ) -> None:
        """
        Save evidence unit embeddings to storage.

        Args:
            resume_fingerprint: Unique identifier for the resume
            evidence_units: List of evidence unit dicts with embedding data
        """
        pass

    @abstractmethod
    def get_evidence_unit_embeddings(
        self,
        resume_fingerprint: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve evidence unit embeddings from storage.

        Args:
            resume_fingerprint: Unique identifier for the resume

        Returns:
            List of evidence unit dicts with embedding data
        """
        pass


class InMemoryEmbeddingStore:
    """In-memory implementation of embedding store for testing."""

    def __init__(self):
        """Initialize in-memory storage."""
        self._storage: Dict[str, List[Dict[str, Any]]] = {}

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[Dict[str, Any]]
    ) -> None:
        """Save sections to in-memory storage."""
        if resume_fingerprint not in self._storage:
            self._storage[resume_fingerprint] = []
        self._storage[resume_fingerprint].extend(sections)

    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> List[Any]:
        """Retrieve sections from in-memory storage."""
        sections = self._storage.get(resume_fingerprint, [])
        if section_type is not None:
            return [s for s in sections if s.get('section_type') == section_type]
        return sections

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[Dict[str, Any]]
    ) -> None:
        """Save evidence units to in-memory storage."""
        if resume_fingerprint not in self._storage:
            self._storage[resume_fingerprint] = []
        self._storage[resume_fingerprint].extend(evidence_units)

    def get_evidence_unit_embeddings(
        self,
        resume_fingerprint: str
    ) -> List[Dict[str, Any]]:
        """Retrieve evidence units from in-memory storage."""
        return self._storage.get(resume_fingerprint, [])

    def clear(self) -> None:
        """Clear all stored data."""
        self._storage.clear()


class JobRepositoryAdapter:
    """Adapter that wraps JobRepository to implement ResumeSectionEmbeddingStore."""

    def __init__(self, job_repository):
        """
        Initialize adapter with JobRepository.

        Args:
            job_repository: Instance of JobRepository to wrap
        """
        self._repo = job_repository

    def save_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        sections: List[Dict[str, Any]]
    ) -> None:
        """Delegate to JobRepository.save_resume_section_embeddings."""
        self._repo.save_resume_section_embeddings(
            resume_fingerprint=resume_fingerprint,
            sections=sections
        )

    def get_resume_section_embeddings(
        self,
        resume_fingerprint: str,
        section_type: Optional[str] = None
    ) -> List[Any]:
        """Delegate to JobRepository.get_resume_section_embeddings."""
        return self._repo.get_resume_section_embeddings(
            resume_fingerprint=resume_fingerprint,
            section_type=section_type
        )

    def save_evidence_unit_embeddings(
        self,
        resume_fingerprint: str,
        evidence_units: List[Dict[str, Any]]
    ) -> None:
        """Delegate to JobRepository.save_evidence_unit_embeddings."""
        if hasattr(self._repo, 'save_evidence_unit_embeddings'):
            self._repo.save_evidence_unit_embeddings(
                resume_fingerprint=resume_fingerprint,
                evidence_units=evidence_units
            )
