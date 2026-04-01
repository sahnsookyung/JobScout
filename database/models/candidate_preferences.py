from sqlalchemy import Column, Integer, Text, TIMESTAMP, ForeignKey, Boolean
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB

from .base import Base


class CandidatePreferences(Base):
    """Per-user matching preferences and hard constraints."""

    __tablename__ = "candidate_preferences"

    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    remote_mode = Column(Text, nullable=False, default="any")
    target_locations = Column(JSONB, nullable=False, default=list)
    visa_sponsorship_required = Column(Boolean, nullable=False, default=False)
    salary_min = Column(Integer)
    employment_types = Column(JSONB, nullable=False, default=list)
    soft_preferences = Column(Text, nullable=False, default="")
    soft_preference_summary = Column(Text)
    preference_mode = Column(Text, nullable=False, default="semantic_rerank")
    preference_profile = Column(JSONB)
    revision = Column(Integer, nullable=False, default=0)
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text("timezone('UTC', now())"),
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text("timezone('UTC', now())"),
        onupdate=sql_text("timezone('UTC', now())"),
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
