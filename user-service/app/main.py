from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List
import redis
import json

from . import models, schemas, crud
from .database import engine, get_db
from .cache import get_redis
from .messaging import publish_event

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="User Service", version="1.0.0")


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "user-service"}


@app.post("/users/", response_model=schemas.User, status_code=201)
def create_user(user: schemas.UserCreate, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    existing = crud.get_user_by_email(db, user.email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    db_user = crud.create_user(db, user)
    r.delete("users:all")
    publish_event("user.created", {
        "user_id": db_user.id,
        "username": db_user.username,
        "email": db_user.email
    })
    return db_user


@app.get("/users/", response_model=List[schemas.User])
def list_users(db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    cached = r.get("users:all")
    if cached:
        return json.loads(cached)
    users = crud.get_users(db)
    pydantic_users = [schemas.User.from_orm(u) for u in users]
    r.setex("users:all", 60, json.dumps([u.dict() for u in pydantic_users], default=str))
    return users


@app.get("/users/{user_id}", response_model=schemas.User)
def get_user(user_id: int, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    cache_key = f"user:{user_id}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)
    user = crud.get_user(db, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    pydantic_user = schemas.User.from_orm(user)
    r.setex(cache_key, 60, json.dumps(pydantic_user.dict(), default=str))
    return user


@app.put("/users/{user_id}", response_model=schemas.User)
def update_user(user_id: int, user: schemas.UserCreate, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    db_user = crud.update_user(db, user_id, user)
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    r.delete(f"user:{user_id}", "users:all")
    return db_user


@app.delete("/users/{user_id}", status_code=204)
def delete_user(user_id: int, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    db_user = crud.get_user(db, user_id)
    if not db_user:
        raise HTTPException(status_code=404, detail="User not found")
    crud.delete_user(db, user_id)
    r.delete(f"user:{user_id}", "users:all")
    publish_event("user.deleted", {"user_id": user_id})
