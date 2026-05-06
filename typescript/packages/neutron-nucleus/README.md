# @neutron-build/nucleus

TypeScript client for the Nucleus database.

14 data models over PostgreSQL wire protocol — SQL, KV, Vector, TimeSeries, Document, Graph, FTS, Geo, Blob, Streams, PubSub, Columnar, Datalog, CDC.

## Installation

```bash
npm install @neutron-build/nucleus
```

## Usage

```typescript
import { createClient } from "@neutron-build/nucleus";
import { query } from "@neutron-build/nucleus/sql";
import { vectorSearch } from "@neutron-build/nucleus/vector";
```

## Documentation

[neutron.build](https://neutron.build)

## License

MIT
