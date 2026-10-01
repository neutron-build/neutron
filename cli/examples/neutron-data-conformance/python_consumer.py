"""Public native Python reads; generated scalars and separate temporal model."""
import asyncio, json, os
from pathlib import Path
import neutron.nucleus.client as native_client
from datetime import datetime
from decimal import Decimal
from uuid import UUID
from pydantic import BaseModel, ValidationError
from neutron.nucleus.client import NucleusClient
from scalars import Scalars

class Temporal(BaseModel):
    happened: datetime

async def main():
    assert Path(native_client.__file__).resolve() == Path(os.environ['CONFORMANCE_PY_SOURCE']).resolve()
    db = await NucleusClient.connect(os.environ["DATABASE_URL"])
    try:
        rows = await db.sql.query(Scalars, "SELECT * FROM fixture.scalars ORDER BY row_id")
        assert isinstance(rows[0].i8, int) and isinstance(rows[0].amount, Decimal)
        assert isinstance(rows[0].ident, UUID) and isinstance(rows[0].payload, bytes)
        missing = rows[2].model_dump()
        del missing['payload']
        try:
            Scalars.model_validate(missing)
        except ValidationError as exc:
            assert any(e['loc'] == ('payload',) and e['type'] == 'missing' for e in exc.errors())
        else:
            raise AssertionError('nullable field must still be present')
        null = rows[2]
        assert all(getattr(null, k) is None for k in Scalars.model_fields if k != "row_id")
        def norm(r):
            return {k: (v.hex() if isinstance(v, bytes) else str(v) if isinstance(v, (Decimal, UUID)) or k == "i8" and v is not None else v) for k, v in r.model_dump().items()}
        temporal = await db.sql.query_one(Temporal, "SELECT happened FROM fixture.temporal")
        assert temporal.happened.utcoffset().total_seconds() == 0
        print(json.dumps({"rows": [norm(r) for r in rows], "temporal": temporal.happened.isoformat(timespec="microseconds")}))
    finally:
        await db.close()
asyncio.run(main())
