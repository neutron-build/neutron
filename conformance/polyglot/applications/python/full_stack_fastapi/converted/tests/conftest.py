# Derived from fastapi/full-stack-fastapi-template (MIT, Copyright (c) 2019 Sebastian
# Ramirez), backend/tests/conftest.py. The db fixture is a Neutron ORM DbSession that
# re-reads tracked objects (fresh_reads) so a long-lived Session observes commits made by
# the application's own connections; everything else is the template's.
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient

from app.core.config import settings
from app.core.db import init_db
from app.main import app
from app.persistence import ITEM_TABLE, USER_TABLE, DbSession, all_rows
from neutron.orm import delete
from tests.utils.user import authentication_token_from_email
from tests.utils.utils import get_superuser_token_headers


@pytest.fixture(scope="session", autouse=True)
def db() -> Generator[DbSession]:
    session = DbSession.open(fresh_reads=True)
    try:
        init_db(session)
        yield session
        session.execute(delete(ITEM_TABLE, where=all_rows(ITEM_TABLE)))
        session.execute(delete(USER_TABLE, where=all_rows(USER_TABLE)))
        session.commit()
    finally:
        session.close()


@pytest.fixture(scope="module")
def client() -> Generator[TestClient]:
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def superuser_token_headers(client: TestClient) -> dict[str, str]:
    return get_superuser_token_headers(client)


@pytest.fixture(scope="module")
def normal_user_token_headers(client: TestClient, db: DbSession) -> dict[str, str]:
    return authentication_token_from_email(
        client=client, email=settings.EMAIL_TEST_USER, db=db
    )
