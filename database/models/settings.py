from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, func
from sqlalchemy.sql import text as sql_text

from .base import Base

class AppSettings(Base):
    __tablename__ = 'app_settings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(255), unique=True, nullable=False, index=True)
    value = Column(Text)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=sql_text("timezone('UTC', now())"), onupdate=func.now())
