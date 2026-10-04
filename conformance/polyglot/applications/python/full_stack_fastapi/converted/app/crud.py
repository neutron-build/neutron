# Derived from fastapi/full-stack-fastapi-template (MIT, Copyright (c) 2019 Sebastian
# Ramirez), backend/app/crud.py. Converted from SQLModel to the Neutron ORM; the
# function signatures, password handling and timing behavior are kept.
import uuid
from typing import Any

from neutron.orm import select

from app.core.security import get_password_hash, verify_password
from app.models import ItemCreate, UserCreate, UserUpdate
from app.persistence import (
    ITEM_OWNER_ID,
    USER_EMAIL,
    USER_ID,
    DbSession,
    Item,
    User,
)


def create_user(*, session: DbSession, user_create: UserCreate) -> User:
    db_obj = User(
        email=user_create.email,
        is_active=user_create.is_active,
        is_superuser=user_create.is_superuser,
        full_name=user_create.full_name,
        hashed_password=get_password_hash(user_create.password),
    )
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def update_user(*, session: DbSession, db_user: User, user_in: UserUpdate) -> Any:
    user_data = user_in.model_dump(exclude_unset=True)
    for name in ("email", "is_active", "is_superuser", "full_name"):
        if name in user_data:
            setattr(db_user, name, user_data[name])
    if "password" in user_data:
        db_user.hashed_password = get_password_hash(user_data["password"])
    session.commit()
    session.refresh(db_user)
    return db_user


def get_user_by_email(*, session: DbSession, email: str) -> User | None:
    # The email column is unique, so at most one id comes back; the tracked object is
    # then loaded through the Session identity map.
    user_id = session.one_or_none(select(USER_ID).where(USER_EMAIL.eq(email)))
    return None if user_id is None else session.get(User, user_id)


# Dummy hash to use for timing attack prevention when user is not found
# This is an Argon2 hash of a random password, used to ensure constant-time comparison
DUMMY_HASH = "$argon2id$v=19$m=65536,t=3,p=4$MjQyZWE1MzBjYjJlZTI0Yw$YTU4NGM5ZTZmYjE2NzZlZjY0ZWY3ZGRkY2U2OWFjNjk"


def authenticate(*, session: DbSession, email: str, password: str) -> User | None:
    db_user = get_user_by_email(session=session, email=email)
    if not db_user:
        # Prevent timing attacks by running password verification even when user doesn't exist
        # This ensures the response time is similar whether or not the email exists
        verify_password(password, DUMMY_HASH)
        return None
    verified, updated_password_hash = verify_password(password, db_user.hashed_password)
    if not verified:
        return None
    if updated_password_hash:
        # db_user is already tracked by the Session, so there is no add().
        db_user.hashed_password = updated_password_hash
        session.commit()
        session.refresh(db_user)
    return db_user


def create_item(*, session: DbSession, item_in: ItemCreate, owner_id: uuid.UUID) -> Item:
    db_item = Item(title=item_in.title, description=item_in.description, owner_id=owner_id)
    session.add(db_item)
    session.commit()
    session.refresh(db_item)
    return db_item


# Helpers below have no counterpart in the template's crud module: they hold the
# multi-step persistence sequences its routes ran inline, so each route can run one
# synchronous call on the ORM thread.


def save(*, session: DbSession, obj: object) -> None:
    session.commit()
    session.refresh(obj)


def delete_user(*, session: DbSession, user: User) -> None:
    # The template deletes owned items explicitly (and relies on cascade_delete for
    # /users/me); both are one bulk delete here, and the database FK also cascades.
    session.bulk_delete(Item, where=ITEM_OWNER_ID.eq(user.id))
    session.delete(user)
    session.commit()


def delete_item(*, session: DbSession, item: Item) -> None:
    session.delete(item)
    session.commit()
