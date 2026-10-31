# Derived from fastapi/full-stack-fastapi-template (MIT, Copyright (c) 2019 Sebastian
# Ramirez), backend/app/api/routes/items.py. Persistence is the Neutron ORM, run on the
# ORM thread; routes, status codes, details and response models are the template's.
import uuid
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, HTTPException

from app import crud
from app.api.deps import CurrentUser, SessionDep
from app.models import ItemCreate, ItemPublic, ItemsPublic, ItemUpdate, Message
from app.persistence import (
    ITEM_CREATED_AT,
    ITEM_ID,
    ITEM_OWNER_ID,
    ITEM_TABLE,
    Item,
    count_query,
    newest_first,
    rows_query,
    run_db,
)

router = APIRouter(prefix="/items", tags=["items"])


@router.get("/", response_model=ItemsPublic)
async def read_items(
    session: SessionDep, current_user: CurrentUser, skip: int = 0, limit: int = 100
) -> Any:
    """
    Retrieve items.
    """

    count_statement = count_query(ITEM_TABLE, ITEM_ID)
    statement = rows_query(ITEM_TABLE).order_by(newest_first(ITEM_CREATED_AT))
    if not current_user.is_superuser:
        owned = ITEM_OWNER_ID.eq(current_user.id)
        count_statement = count_statement.where(owned)
        statement = statement.where(owned)
    count = await run_db(session.one, count_statement)
    items = await run_db(session.all, statement.offset(skip).limit(limit))

    items_public = [ItemPublic.model_validate(item) for item in items]
    return ItemsPublic(data=items_public, count=count)


@router.get("/{id}", response_model=ItemPublic)
async def read_item(session: SessionDep, current_user: CurrentUser, id: uuid.UUID) -> Any:
    """
    Get item by ID.
    """
    item = await run_db(session.get, Item, id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    if not current_user.is_superuser and (item.owner_id != current_user.id):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    return ItemPublic.model_validate(asdict(item))


@router.post("/", response_model=ItemPublic)
async def create_item(
    *, session: SessionDep, current_user: CurrentUser, item_in: ItemCreate
) -> Any:
    """
    Create new item.
    """
    item = await run_db(
        crud.create_item, session=session, item_in=item_in, owner_id=current_user.id
    )
    return ItemPublic.model_validate(asdict(item))


@router.put("/{id}", response_model=ItemPublic)
async def update_item(
    *,
    session: SessionDep,
    current_user: CurrentUser,
    id: uuid.UUID,
    item_in: ItemUpdate,
) -> Any:
    """
    Update an item.
    """
    item = await run_db(session.get, Item, id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    if not current_user.is_superuser and (item.owner_id != current_user.id):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    update_dict = item_in.model_dump(exclude_unset=True)
    for name, value in update_dict.items():
        setattr(item, name, value)
    await run_db(crud.save, session=session, obj=item)
    return ItemPublic.model_validate(asdict(item))


@router.delete("/{id}")
async def delete_item(
    session: SessionDep, current_user: CurrentUser, id: uuid.UUID
) -> Message:
    """
    Delete an item.
    """
    item = await run_db(session.get, Item, id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    if not current_user.is_superuser and (item.owner_id != current_user.id):
        raise HTTPException(status_code=403, detail="Not enough permissions")
    await run_db(crud.delete_item, session=session, item=item)
    return Message(message="Item deleted successfully")
