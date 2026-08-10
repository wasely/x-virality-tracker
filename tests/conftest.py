from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text

from x_virality.config import Settings
from x_virality.db.models import Base
from x_virality.db.session import Database, create_database


@pytest.fixture
def test_database_url() -> str:
    value = os.getenv("XV_TEST_DATABASE_URL")
    if not value:
        pytest.skip("XV_TEST_DATABASE_URL is not configured")
    return value


@pytest_asyncio.fixture
async def database(test_database_url: str) -> AsyncIterator[Database]:
    settings = Settings(_env_file=None, database_url=test_database_url)
    database = create_database(settings)
    async with database.engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield database
    finally:
        async with database.engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.execute(text("DROP TABLE IF EXISTS alembic_version"))
        await database.dispose()
