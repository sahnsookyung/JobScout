from sqlalchemy import Column, Integer, Text, TIMESTAMP, ForeignKey, Boolean
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID, JSONB

from .base import Base


UTC_NOW_SQL = "timezone('UTC', now())"


class CandidatePreferences(Base):
    """Per-user matching preferences and hard constraints."""

    __tablename__ = "candidate_preferences"

    owner_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    remote_mode = Column(
        Text,
        nullable=False,
        default="any",
        server_default=sql_text("'any'"),
    )
    target_locations = Column(
        JSONB,
        nullable=False,
        default=list,
        server_default=sql_text("'[]'"),
    )
    visa_sponsorship_required = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=sql_text("FALSE"),
    )
    salary_min = Column(Integer)
    employment_types = Column(
        JSONB,
        nullable=False,
        default=list,
        server_default=sql_text("'[]'"),
    )
    soft_preferences = Column(
        Text,
        nullable=False,
        default="",
        server_default=sql_text("''"),
    )
    soft_preference_summary = Column(Text)
    preference_mode = Column(
        Text,
        nullable=False,
        default="semantic_rerank",
        server_default=sql_text("'semantic_rerank'"),
    )
    preference_rerank_top_n = Column(Integer)
    preference_profile = Column(JSONB)
    result_policy = Column(JSONB)
    ranking_config = Column(JSONB)
    revision = Column(Integer, nullable=False, default=0, server_default=sql_text("0"))
    created_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text(UTC_NOW_SQL),
    )
    updated_at = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sql_text(UTC_NOW_SQL),
        onupdate=sql_text(UTC_NOW_SQL),
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
