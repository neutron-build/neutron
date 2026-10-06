# Derived from fastapi/full-stack-fastapi-template (MIT, Copyright (c) 2019 Sebastian
# Ramirez), backend/app/initial_data.py, converted to a Neutron ORM DbSession.
import logging

from app.core.db import init_db
from app.persistence import DbSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init() -> None:
    session = DbSession.open()
    try:
        init_db(session)
    finally:
        session.close()


def main() -> None:
    logger.info("Creating initial data")
    init()
    logger.info("Initial data created")


if __name__ == "__main__":
    main()
