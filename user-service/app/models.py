from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
from .database import Base


class User(Base):
    __tablename__ = "users"

    id         = Column(Integer, primary_key=True, index=True)
    username   = Column(String(100), unique=True, nullable=False)
    email      = Column(String(255), unique=True, nullable=False, index=True)
    full_name  = Column(String(255), nullable=True)
    is_active  = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
