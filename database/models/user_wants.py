import uuid

from sqlalchemy import Column, Text, TIMESTAMP, Index, ForeignKey
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID
from pgvector.sqlalchemy import Vector

from .base import Base


class UserWants(Base):
    """Stores user-provided wants as individual entries with their embeddings."""

    __tablename__ = 'user_wants'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    owner_id = Column(UUID(as_uuid=True), ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    wants_text = Column(Text, nullable=False)
    embedding = Column(Vector(1024), nullable=False)
    facet_key = Column(Text)
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text("timezone('UTC', now())"),
    )

    __table_args__ = (
        Index('idx_user_wants_owner', 'owner_id'),
    )

    def __init__(self, **kwargs):
        if "user_id" in kwargs and "owner_id" not in kwargs:
            kwargs["owner_id"] = kwargs.pop("user_id")
        super().__init__(**kwargs)

    @property
    def user_id(self):
        return self.owner_id

    @user_id.setter
    def user_id(self, value):
        self.owner_id = value
