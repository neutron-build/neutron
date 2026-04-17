import MetricCard from "../components/MetricCard";
import FeatureGrid from "../components/FeatureGrid";
import BenchmarkBars from "../components/BenchmarkBars";
import SectionBreak from "../components/SectionBreak";

export default function NucleusPage() {
  return (
    <>
      <main id="main-content" class="nucleus-page">
        {/* HERO */}
        <header class="nucleus-hero">
          <div class="nucleus-hero__glow" aria-hidden="true"></div>
          <div class="nucleus-hero__grid-bg" aria-hidden="true"></div>
          <div class="container nucleus-hero__inner">
            <div class="nucleus-hero__badge" data-animate>DATABASE</div>
            <h1 class="nucleus-hero__title" data-animate style={{ "--animate-delay": "0.1s" } as any}>
              14 engines. 1 system.
            </h1>
            <p class="nucleus-hero__desc" data-animate style={{ "--animate-delay": "0.15s" } as any}>
              Stop running Postgres, Redis, Elasticsearch, TimescaleDB, Neo4j, and MongoDB as separate services. Nucleus gives you 14 specialized storage engines in one process — each purpose-built for its data model. Built in Rust with 3,724 passing tests.
            </p>
            <div class="nucleus-hero__metrics" data-animate style={{ "--animate-delay": "0.2s" } as any}>
              <MetricCard value="3,724" label="Tests Passing" variant="excellent" />
              <MetricCard value="14" label="Data Models" variant="good" />
              <MetricCard value="ACID" label="Transactions" variant="neutral" />
              <MetricCard value="Rust" label="Built With" variant="neutral" />
            </div>
          </div>
        </header>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* ENGINE ARCHITECTURE */}
        <section class="nucleus-section">
          <div class="container">
            <h2 class="nucleus-section__title" data-animate>Native Multi-Storage Architecture</h2>
            <p class="nucleus-section__desc" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              Nucleus isn't a row-based database with extensions. It's <strong>14 specialized data models</strong> in one system, each optimized for its workload.
            </p>
          </div>
          <FeatureGrid columns={3} accentRgb="16, 185, 129">
            <div class="feature-card">
              <div class="feature-card__title">SQL (Row-Based)</div>
              <div class="feature-card__desc">B-tree indexes, MVCC, WAL, PostgreSQL wire protocol. Full OLTP with joins, subqueries, CTEs.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">OLAP (Columnar)</div>
              <div class="feature-card__desc">Columnar compression, vectorized execution, SIMD scans. 5.9x faster than PostgreSQL for SUM queries.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Vector (HNSW)</div>
              <div class="feature-card__desc">Hierarchical Navigable Small World + IVFFlat for approximate nearest neighbor search.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Graph (CSR)</div>
              <div class="feature-card__desc">Compressed Sparse Row with index-free adjacency for traversals. Efficient graph storage.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Timeseries</div>
              <div class="feature-card__desc">Delta compression, time-based partitioning, downsampling. Purpose-built for metrics and IoT.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Document</div>
              <div class="feature-card__desc">JSON collections with B-tree indexing and schema validation. Flexible document storage.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Key-Value</div>
              <div class="feature-card__desc">LSM trees with TTL, atomic operations. SQL functions for get, set, delete, increment, and expiry.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Full-Text Search</div>
              <div class="feature-card__desc">BM25 ranking, stemming, highlighting, fuzzy matching. Inverted index architecture.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Geo (R-trees)</div>
              <div class="feature-card__desc">Spatial indexing for radius, polygon, and nearest-neighbor queries.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Blob Storage</div>
              <div class="feature-card__desc">Chunked, content-addressed blob storage with deduplication. Store files alongside structured data.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Streams</div>
              <div class="feature-card__desc">Append-only log with consumer groups. Redis-compatible stream semantics for event sourcing.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">Datalog</div>
              <div class="feature-card__desc">Semi-naive evaluation with SQL integration. Recursive queries and rule-based reasoning.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">CDC</div>
              <div class="feature-card__desc">Change Data Capture for real-time data pipelines. Track every insert, update, and delete.</div>
            </div>
            <div class="feature-card">
              <div class="feature-card__title">PubSub</div>
              <div class="feature-card__desc">LISTEN/NOTIFY for real-time event broadcasting. Push updates to connected clients instantly.</div>
            </div>
          </FeatureGrid>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* PERFORMANCE */}
        <section class="nucleus-section nucleus-section--dark">
          <div class="container">
            <h2 class="nucleus-section__title" data-animate>Performance Targets</h2>
            <p class="nucleus-section__desc" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              Native storage for each model means Nucleus matches or exceeds specialist databases.
            </p>
          </div>
          <BenchmarkBars
            title="Columnar Engine vs PostgreSQL (measured)"
            bars={[
              { label: 'SUM queries', value: '5.9x faster', width: 100, color: '#10B981' },
              { label: 'COUNT(*)', value: '4.2x faster', width: 71, color: '#34D399' },
              { label: 'INSERT', value: '1.6x faster', width: 27, color: '#6EE7B7' },
              { label: 'GROUP BY', value: '1.2x faster', width: 20, color: '#A7F3D0' },
              { label: 'Point query', value: '1.1x faster', width: 18, color: '#D1FAE5' },
            ]}
          />
          <p class="nucleus-section__note container" data-animate>
            <em>Columnar engine benchmarks measured against PostgreSQL. Other engine comparisons in progress.</em>
          </p>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* DATABASE BRANCHING */}
        <section class="nucleus-section">
          <div class="container">
            <h2 class="nucleus-section__title" data-animate>Database Branching <span style={{ fontSize: "0.5em", color: "var(--text-tertiary)", fontWeight: 500 }}>(Planned)</span></h2>
            <p class="nucleus-section__desc" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              Git for your data. Create isolated branches, test schema changes, then merge back to main. Copy-on-write ensures branches share unchanged data.
            </p>
          </div>
          <div class="branching-visual container" data-animate style={{ "--animate-delay": "0.1s" } as any}>
            <div class="branch-diagram">
              <div class="branch-line branch-line--main">
                <span class="branch-label">main</span>
                <div class="branch-node branch-node--filled"></div>
                <div class="branch-connector"></div>
                <div class="branch-node branch-node--filled"></div>
                <div class="branch-connector"></div>
                <div class="branch-node branch-node--merge"></div>
              </div>
              <div class="branch-line branch-line--feature">
                <span class="branch-label">feature/auth</span>
                <div class="branch-node branch-node--fork"></div>
                <div class="branch-connector branch-connector--dashed"></div>
                <div class="branch-node branch-node--filled"></div>
                <div class="branch-connector branch-connector--dashed"></div>
                <div class="branch-node branch-node--merge-source"></div>
              </div>
            </div>
            <div class="branching-uses">
              <div class="branching-use">Test migrations without downtime</div>
              <div class="branching-use">Debug production issues in isolation</div>
              <div class="branching-use">Staging environments sharing production data</div>
              <div class="branching-use">Temporal queries from historical snapshots</div>
            </div>
          </div>
        </section>

        {/* BEYOND THE ENGINES */}
        <section class="nucleus-section">
          <div class="container container--code">
            <h2 class="nucleus-section__title" data-animate>Beyond the Engines</h2>
            <div class="production-prose" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              <p><strong>Database branching</strong> (planned) — Git for your data. Create isolated branches, test schema changes, then merge back. Copy-on-write ensures branches share unchanged data.</p>
              <p><strong>Distributed consensus</strong> — Raft replication with automatic cluster query forwarding.</p>
              <p><strong>Encryption & compression</strong> — AES-256 encryption at rest (<code>--encrypt</code> CLI flag). LZ4 compression for storage (<code>--compress</code> CLI flag).</p>
              <p><strong>SIMD acceleration</strong> — Vectorized AVG/MIN/MAX fast paths for columnar queries. 128MB buffer pool with background flush.</p>
            </div>
          </div>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* PRODUCTION */}
        <section class="nucleus-section">
          <div class="container container--code">
            <h2 class="nucleus-section__title" data-animate>Production-Grade Engineering</h2>
            <div class="production-prose" data-animate style={{ "--animate-delay": "0.05s" } as any}>
              <p><strong>3,724 tests passing</strong> — unit, integration, and property-based testing with proptest.</p>
              <p><strong>Rust 2024 Edition</strong> — memory safety without garbage collection. No segfaults, no data races, no null pointer exceptions.</p>
              <p><strong>PostgreSQL compatible</strong> — use existing Postgres drivers and tools via pgwire 0.36. Drop-in replacement for OLTP workloads.</p>
              <p><strong>SQL parsing</strong> — powered by sqlparser 0.61 with extensions for multi-model SQL functions.</p>
              <p><strong>Comprehensive benchmarks</strong> — Criterion benchmarks for query, storage, and specialty workloads. Columnar engine benchmarked against PostgreSQL.</p>
            </div>
          </div>
        </section>

        <SectionBreak accentRgb="16, 185, 129" />

        {/* CTA */}
        <section class="nucleus-cta">
          <div class="container container--narrow">
            <h2 class="nucleus-cta__title" data-animate>Get Started</h2>
            <div class="nucleus-cta__terminal" data-animate style={{ "--animate-delay": "0.1s" } as any}>
              <div class="terminal-bar">
                <span class="terminal-dot"></span>
                <span class="terminal-dot"></span>
                <span class="terminal-dot"></span>
              </div>
              <pre class="terminal-body"><code><span class="terminal-prompt">$</span> git clone &amp;&amp; cargo build --release
<span class="terminal-prompt">$</span> ./target/release/nucleus --port 5432</code></pre>
            </div>
          </div>
        </section>
      </main>
    </>
  );
}
