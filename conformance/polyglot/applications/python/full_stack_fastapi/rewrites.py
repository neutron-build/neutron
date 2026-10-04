"""Exact source rewrites that derive the converted test suite from the frozen template tests.

Every rewrite names the file, the exact original text, its replacement and how many
times it must occur; prepare_converted.py and verify_inventory.py fail if a count
differs, so a changed upstream byte or an unmatched rewrite cannot pass silently. The
only things that change are imports of the SQLModel Session/select and the handful of
statements that read the database through SQLModel; every assertion is untouched.
"""

PERSISTENCE_SESSION = "from app.persistence import DbSession as Session\n"

# path (relative to backend/) -> [(old, new, expected_count)]
REWRITES = {
    "tests/utils/item.py": [
        ("from sqlmodel import Session\n", PERSISTENCE_SESSION, 1),
    ],
    "tests/utils/user.py": [
        ("from sqlmodel import Session\n", PERSISTENCE_SESSION, 1),
    ],
    "tests/crud/test_user.py": [
        ("from sqlmodel import Session\n", PERSISTENCE_SESSION, 1),
        (
            "from app.models import User, UserCreate, UserUpdate\n",
            "from app.models import UserCreate, UserUpdate\nfrom app.persistence import User\n",
            1,
        ),
    ],
    "tests/api/routes/test_items.py": [
        ("from sqlmodel import Session\n", PERSISTENCE_SESSION, 1),
    ],
    "tests/api/routes/test_login.py": [
        ("from sqlmodel import Session\n", PERSISTENCE_SESSION, 1),
        (
            "from app.models import User, UserCreate\n",
            "from app.models import UserCreate\nfrom app.persistence import User\n",
            1,
        ),
    ],
    "tests/api/routes/test_private.py": [
        (
            "from fastapi.testclient import TestClient\nfrom sqlmodel import Session, select\n",
            "from uuid import UUID\n\nfrom fastapi.testclient import TestClient\n" + PERSISTENCE_SESSION,
            1,
        ),
        ("from app.models import User\n", "from app.persistence import User\n", 1),
        (
            'user = db.exec(select(User).where(User.id == data["id"])).first()',
            'user = db.get(User, UUID(data["id"]))',
            1,
        ),
    ],
    "tests/api/routes/test_users.py": [
        ("from sqlmodel import Session, select\n", PERSISTENCE_SESSION, 1),
        (
            "from app.models import User, UserCreate\n",
            "from app.models import UserCreate\nfrom app.persistence import User\n",
            1,
        ),
        (
            "    user_query = select(User).where(User.email == email)\n"
            "    user_db = db.exec(user_query).first()\n",
            "    user_db = crud.get_user_by_email(session=db, email=email)\n",
            1,
        ),
        (
            "    user_query = select(User).where(User.email == settings.FIRST_SUPERUSER)\n"
            "    user_db = db.exec(user_query).first()\n",
            "    user_db = crud.get_user_by_email(session=db, email=settings.FIRST_SUPERUSER)\n",
            1,
        ),
        (
            "    user_query = select(User).where(User.email == username)\n"
            "    user_db = db.exec(user_query).first()\n",
            "    user_db = crud.get_user_by_email(session=db, email=username)\n",
            2,
        ),
        (
            "    result = db.exec(select(User).where(User.id == user_id)).first()\n",
            "    result = db.get(User, user_id)\n",
            2,
        ),
        (
            "    user_query = select(User).where(User.id == user_id)\n"
            "    user_db = db.execute(user_query).first()\n",
            "    user_db = db.get(User, user_id)\n",
            1,
        ),
    ],
}
