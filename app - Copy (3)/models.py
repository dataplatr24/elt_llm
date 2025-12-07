# Placeholder for SQLAlchemy models if you want to persist app-level data
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, JSON
from datetime import datetime

Base = declarative_base()

class AnalysisRun(Base):
    __tablename__ = "analysis_runs"
    id = Column(Integer, primary_key=True, index=True)
    user = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    metadata = Column(JSON)
    result_text = Column(String)