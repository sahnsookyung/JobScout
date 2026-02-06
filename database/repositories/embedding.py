from typing import List, Optional, Tuple
from sqlalchemy import select

from database.models import ResumeSectionEmbedding
from database.repositories.base import BaseRepository
from core.utils import cosine_similarity_from_distance


class EmbeddingRepository(BaseRepository):
    def find_similar_resume_sections(
        self,
        query_embedding: List[float],
        section_type: Optional[str] = None,
        top_k: int = 10
    ) -> List[Tuple[ResumeSectionEmbedding, float]]:
        stmt = select(
            ResumeSectionEmbedding,
            ResumeSectionEmbedding.embedding.cosine_distance(query_embedding).label('distance')
        )

        if section_type:
            stmt = stmt.where(ResumeSectionEmbedding.section_type == section_type)

        stmt = stmt.order_by('distance').limit(top_k)

        results = self.db.execute(stmt).all()
        return [(row[0], cosine_similarity_from_distance(row._mapping['distance'])) for row in results]
