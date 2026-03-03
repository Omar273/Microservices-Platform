from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class ItemBase(BaseModel):
    name:        str            = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    price:       float          = Field(..., gt=0)
    in_stock:    bool           = True


class ItemCreate(ItemBase):
    pass


class Item(ItemBase):
    id:         int
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    class Config:
        orm_mode        = True
        from_attributes = True
