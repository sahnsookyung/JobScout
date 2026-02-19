import uuid

from sqlalchemy import Column, Text, TIMESTAMP
from sqlalchemy.sql import text as sql_text
from sqlalchemy.dialects.postgresql import UUID

from .base import Base

class Tenant(Base):
    __tablename__ = 'tenant'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"))
