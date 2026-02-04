from typing import List, Optional, Any
from sqlalchemy import select

from database.models import ResumeSectionEmbedding
from database.repositories.base import BaseRepository


class EmbeddingRepository(BaseRepository):
    def find_similar_resume_sections(
        self,
        query_embedding: List[float],
        section_type: Optional[str] = None,
        top_k: int = 10
    ) -> List[tuple]:
        stmt = select(
            ResumeSectionEmbedding,
            ResumeSectionEmbedding.embedding.cosine_distance(query_embedding).label('distance')
        )

        if section_type:
            stmt = stmt.where(ResumeSectionEmbedding.section_type == section_type)

        stmt = stmt.order_by('distance').limit(top_k)

        results = self.db.execute(stmt).all()
        return [(row.ResumeSectionEmbedding, 1.0 - row.distance) for row in results]
