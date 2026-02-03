#!/usr/bin/env python3
"""
Resume Section Embedding Store - Interface for persisting resume section embeddings.

Provides abstraction layer for persistence operations.
"""
from typing import Protocol, runtime_checkable, List, Dict, Any, Optional
from dataclasses import dataclass


@runtime_checkable
class ResumeSectionEmbeddingStore(Protocol):
    """Protocol for storing resume section embeddings."""
    
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
        ...
    
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
        ...


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
        section_type: str = None
    ) -> List[Any]:
        """Retrieve sections from in-memory storage."""
        sections = self._storage.get(resume_fingerprint, [])
        if section_type is not None:
            return [s for s in sections if s.get('section_type') == section_type]
        return sections
    
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
        section_type: str = None
    ) -> List[Any]:
        """Delegate to JobRepository.get_resume_section_embeddings."""
        return self._repo.get_resume_section_embeddings(
            resume_fingerprint=resume_fingerprint,
            section_type=section_type
        )
