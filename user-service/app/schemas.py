from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class UserBase(BaseModel):
    username:  str           = Field(..., min_length=3, max_length=100)
    email:     str           = Field(..., max_length=255)
    full_name: Optional[str] = Field(None, max_length=255)
    is_active: bool          = True


class UserCreate(UserBase):
    pass


class User(UserBase):
    id:         int
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    class Config:
        orm_mode        = True
        from_attributes = True
