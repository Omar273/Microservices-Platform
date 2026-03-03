from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, func
from .database import Base


class Item(Base):
    __tablename__ = "items"

    id          = Column(Integer, primary_key=True, index=True)
    name        = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)
    price       = Column(Float, nullable=False)
    in_stock    = Column(Boolean, default=True)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True), onupdate=func.now())
