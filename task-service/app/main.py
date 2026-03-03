from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List
import redis
import json

from . import models, schemas, crud
from .database import engine, get_db
from .cache import get_redis

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Task Service", version="1.0.0")


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "task-service"}


@app.post("/items/", response_model=schemas.Item, status_code=201)
def create_item(item: schemas.ItemCreate, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    db_item = crud.create_item(db, item)
    r.delete("items:all")
    return db_item


@app.get("/items/", response_model=List[schemas.Item])
def list_items(db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    cached = r.get("items:all")
    if cached:
        return json.loads(cached)
    items = crud.get_items(db)
    pydantic_items = [schemas.Item.from_orm(i) for i in items]
    r.setex("items:all", 60, json.dumps([i.dict() for i in pydantic_items], default=str))
    return items


@app.get("/items/{item_id}", response_model=schemas.Item)
def get_item(item_id: int, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    cache_key = f"item:{item_id}"
    cached = r.get(cache_key)
    if cached:
        return json.loads(cached)
    item = crud.get_item(db, item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    pydantic_item = schemas.Item.from_orm(item)
    r.setex(cache_key, 60, json.dumps(pydantic_item.dict(), default=str))
    return item


@app.put("/items/{item_id}", response_model=schemas.Item)
def update_item(item_id: int, item: schemas.ItemCreate, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    db_item = crud.update_item(db, item_id, item)
    if not db_item:
        raise HTTPException(status_code=404, detail="Item not found")
    r.delete(f"item:{item_id}", "items:all")
    return db_item


@app.delete("/items/{item_id}", status_code=204)
def delete_item(item_id: int, db: Session = Depends(get_db), r: redis.Redis = Depends(get_redis)):
    success = crud.delete_item(db, item_id)
    if not success:
        raise HTTPException(status_code=404, detail="Item not found")
    r.delete(f"item:{item_id}", "items:all")
