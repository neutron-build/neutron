# Derived from fastapi/full-stack-fastapi-template (MIT, Copyright (c) 2019 Sebastian
# Ramirez), backend/app/core/db.py. The SQLModel engine is gone: connections are
# opened per DbSession by the Neutron ORM from settings.DATABASE_URL.
from app import crud
from app.core.config import settings
from app.models import UserCreate
from app.persistence import DbSession


def init_db(session: DbSession) -> None:
    # Tables are created with Alembic migrations, as in the template.
    user = crud.get_user_by_email(session=session, email=settings.FIRST_SUPERUSER)
    if not user:
        user_in = UserCreate(
            email=settings.FIRST_SUPERUSER,
            password=settings.FIRST_SUPERUSER_PASSWORD,
            is_superuser=True,
        )
        user = crud.create_user(session=session, user_create=user_in)
