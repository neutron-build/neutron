// JDBC compatibility test — pgjdbc against a release Nucleus server.
//
// Exercises the surfaces JDBC drivers actually stress: extended-protocol
// prepared statements (named server statements + binary transfer past the
// prepare threshold), typed parameters, transactions and PostgreSQL
// error-state semantics, batches, getGeneratedKeys (RETURNING), and
// DatabaseMetaData introspection.
//
// Exit 0 = pass. Any assertion failure or unexpected SQLException = exit 1.

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

public class Main {
    static int checks = 0;

    static void check(boolean cond, String msg) {
        checks++;
        if (!cond) throw new RuntimeException("CHECK FAILED: " + msg);
    }

    static void checkEq(Object got, Object want, String msg) {
        checks++;
        boolean eq = (got == null) ? want == null : got.equals(want);
        if (!eq) throw new RuntimeException("CHECK FAILED: " + msg + " — got=" + got + " want=" + want);
    }

    public static void main(String[] args) throws Exception {
        String url = System.getenv("JDBC_URL");
        if (url == null) throw new RuntimeException("JDBC_URL not set");

        Properties props = new Properties();
        props.setProperty("user", "nucleus");
        // Force the server-prepare threshold low so binary transfer kicks in fast.
        props.setProperty("prepareThreshold", "2");

        try (Connection conn = DriverManager.getConnection(url, props)) {
            sectionBasics(conn);
            sectionTypedPrepared(conn);
            sectionBinaryTransfer(conn);
            sectionTransactions(conn);
            sectionBatch(conn);
            sectionGeneratedKeys(conn);
            sectionMetadata(conn);
            if ("1".equals(System.getenv("NUCLEUS_TEST_CANCEL"))) sectionCancel(conn);
        }
        System.out.println("JDBC: all " + checks + " checks passed");
    }

    static void sectionBasics(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_basic");
            st.execute("CREATE TABLE jdbc_basic (id INT PRIMARY KEY, name TEXT)");
            int n = st.executeUpdate("INSERT INTO jdbc_basic VALUES (1, 'one'), (2, 'two')");
            checkEq(n, 2, "insert rowcount");
            try (ResultSet rs = st.executeQuery("SELECT id, name FROM jdbc_basic ORDER BY id")) {
                check(rs.next(), "row 1 present");
                checkEq(rs.getInt(1), 1, "basic id 1");
                checkEq(rs.getString(2), "one", "basic name 1");
                check(rs.next(), "row 2 present");
                checkEq(rs.getInt("id"), 2, "basic id 2 by name");
                check(!rs.next(), "no third row");
            }
        }
        System.out.println("  basics: ok");
    }

    static void sectionTypedPrepared(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_types");
            st.execute("CREATE TABLE jdbc_types (" +
                    "id INT PRIMARY KEY, i64 BIGINT, t TEXT, b BOOLEAN, f DOUBLE PRECISION, " +
                    "n NUMERIC(12,4), ts TIMESTAMP, d DATE, raw BYTEA, missing TEXT)");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO jdbc_types VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            ps.setInt(1, 7);
            ps.setLong(2, 9_007_199_254_740_993L);
            ps.setString(3, "héllo — utf8 ✓ back\\slash");
            ps.setBoolean(4, true);
            ps.setDouble(5, 2.5);
            ps.setBigDecimal(6, new BigDecimal("12345678.9012"));
            ps.setTimestamp(7, Timestamp.valueOf("2026-07-23 14:30:45.123456"));
            ps.setDate(8, Date.valueOf("1999-12-31"));
            ps.setBytes(9, new byte[]{0, 1, 2, (byte) 0xFF, 0x5C});
            ps.setNull(10, Types.VARCHAR);
            checkEq(ps.executeUpdate(), 1, "typed insert rowcount");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT i64, t, b, f, n, ts, d, raw, missing FROM jdbc_types WHERE id = ?")) {
            ps.setInt(1, 7);
            try (ResultSet rs = ps.executeQuery()) {
                check(rs.next(), "typed row present");
                checkEq(rs.getLong(1), 9_007_199_254_740_993L, "bigint round-trip");
                checkEq(rs.getString(2), "héllo — utf8 ✓ back\\slash", "text round-trip");
                checkEq(rs.getBoolean(3), true, "bool round-trip");
                checkEq(rs.getDouble(4), 2.5, "float8 round-trip");
                check(rs.getBigDecimal(5).compareTo(new BigDecimal("12345678.9012")) == 0,
                        "numeric round-trip, got=" + rs.getBigDecimal(5));
                checkEq(rs.getTimestamp(6), Timestamp.valueOf("2026-07-23 14:30:45.123456"),
                        "timestamp round-trip");
                checkEq(rs.getDate(7).toString(), "1999-12-31", "date round-trip");
                byte[] raw = rs.getBytes(8);
                check(raw.length == 5 && raw[3] == (byte) 0xFF && raw[4] == 0x5C,
                        "bytea round-trip");
                check(rs.getString(9) == null && rs.wasNull(), "null round-trip");
            }
        }
        System.out.println("  typed prepared: ok");
    }

    // Run the same prepared select past the prepare threshold so pgjdbc names
    // the server statement and switches result transfer to BINARY format.
    static void sectionBinaryTransfer(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, i64, t, b, f, n, ts, d, raw FROM jdbc_types WHERE id = ?")) {
            for (int round = 1; round <= 6; round++) {
                ps.setInt(1, 7);
                try (ResultSet rs = ps.executeQuery()) {
                    check(rs.next(), "binary round " + round + " row present");
                    checkEq(rs.getInt(1), 7, "binary round " + round + " id");
                    checkEq(rs.getLong(2), 9_007_199_254_740_993L, "binary round " + round + " bigint");
                    checkEq(rs.getString(3), "héllo — utf8 ✓ back\\slash", "binary round " + round + " text");
                    checkEq(rs.getBoolean(4), true, "binary round " + round + " bool");
                    checkEq(rs.getDouble(5), 2.5, "binary round " + round + " float8");
                    check(rs.getBigDecimal(6).compareTo(new BigDecimal("12345678.9012")) == 0,
                            "binary round " + round + " numeric, got=" + rs.getBigDecimal(6));
                    checkEq(rs.getTimestamp(7), Timestamp.valueOf("2026-07-23 14:30:45.123456"),
                            "binary round " + round + " timestamp");
                    checkEq(rs.getDate(8).toString(), "1999-12-31", "binary round " + round + " date");
                    checkEq(rs.getBytes(9).length, 5, "binary round " + round + " bytea len");
                }
            }
        }
        System.out.println("  binary transfer (past prepareThreshold): ok");
    }

    static void sectionTransactions(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        try (Statement st = conn.createStatement()) {
            st.execute("INSERT INTO jdbc_basic VALUES (10, 'rollback-me')");
            conn.rollback();
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM jdbc_basic WHERE id = 10")) {
                rs.next();
                checkEq(rs.getInt(1), 0, "rollback discarded insert");
            }
            conn.commit();

            st.execute("INSERT INTO jdbc_basic VALUES (11, 'commit-me')");
            conn.commit();
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM jdbc_basic WHERE id = 11")) {
                rs.next();
                checkEq(rs.getInt(1), 1, "commit persisted insert");
            }
            conn.commit();

            // PostgreSQL error-state: a failed statement aborts the transaction;
            // every later statement must fail with 25P02 until rollback.
            st.execute("INSERT INTO jdbc_basic VALUES (12, 'pre-error')");
            boolean threw = false;
            try {
                st.execute("INSERT INTO jdbc_basic VALUES (11, 'dup-pk')");
            } catch (SQLException e) {
                threw = true;
                checkEq(e.getSQLState(), "23505", "dup PK sqlstate");
            }
            check(threw, "dup PK threw");
            threw = false;
            try {
                st.executeQuery("SELECT 1");
            } catch (SQLException e) {
                threw = true;
                checkEq(e.getSQLState(), "25P02", "in_failed_sql_transaction sqlstate");
            }
            check(threw, "aborted txn rejects statements");
            conn.rollback();
            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM jdbc_basic WHERE id = 12")) {
                rs.next();
                checkEq(rs.getInt(1), 0, "aborted txn rolled back pre-error insert");
            }
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        System.out.println("  transactions + error state: ok");
    }

    static void sectionBatch(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_batch");
            st.execute("CREATE TABLE jdbc_batch (id INT PRIMARY KEY, v TEXT)");
        }
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_batch VALUES (?, ?)")) {
            for (int i = 1; i <= 50; i++) {
                ps.setInt(1, i);
                ps.setString(2, "v" + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            checkEq(counts.length, 50, "batch result length");
            for (int c : counts) check(c == 1 || c == Statement.SUCCESS_NO_INFO, "batch row count");
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*), min(id), max(id) FROM jdbc_batch")) {
            rs.next();
            checkEq(rs.getInt(1), 50, "batch total");
            checkEq(rs.getInt(2), 1, "batch min");
            checkEq(rs.getInt(3), 50, "batch max");
        }

        // A failing row inside a batch must surface as BatchUpdateException and
        // leave the connection usable.
        boolean threw = false;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_batch VALUES (?, ?)")) {
            ps.setInt(1, 100); ps.setString(2, "ok"); ps.addBatch();
            ps.setInt(1, 1); ps.setString(2, "dup"); ps.addBatch();
            ps.executeBatch();
        } catch (BatchUpdateException e) {
            threw = true;
        }
        check(threw, "failing batch threw BatchUpdateException");
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 1")) {
            check(rs.next(), "connection usable after failed batch");
        }
        System.out.println("  batch: ok");
    }

    static void sectionGeneratedKeys(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jdbc_serial");
            st.execute("CREATE TABLE jdbc_serial (id SERIAL PRIMARY KEY, v TEXT)");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO jdbc_serial (v) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "first");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                check(keys.next(), "generated keys row");
                checkEq(keys.getInt("id"), 1, "first serial id");
            }
            ps.setString(1, "second");
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                check(keys.next(), "generated keys row 2");
                checkEq(keys.getInt("id"), 2, "second serial id");
            }
        }
        System.out.println("  generated keys (RETURNING): ok");
    }

    static void sectionMetadata(Connection conn) throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        System.out.println("  server: " + md.getDatabaseProductName() + " "
                + md.getDatabaseProductVersion());

        boolean sawBasic = false;
        try (ResultSet rs = md.getTables(null, null, "jdbc_%", new String[]{"TABLE"})) {
            while (rs.next()) {
                if ("jdbc_basic".equals(rs.getString("TABLE_NAME"))) sawBasic = true;
            }
        }
        check(sawBasic, "getTables lists jdbc_basic");

        int cols = 0;
        boolean sawI64 = false;
        try (ResultSet rs = md.getColumns(null, null, "jdbc_types", null)) {
            while (rs.next()) {
                cols++;
                if ("i64".equals(rs.getString("COLUMN_NAME"))) {
                    sawI64 = true;
                    checkEq(rs.getInt("DATA_TYPE"), Types.BIGINT, "i64 maps to BIGINT");
                }
            }
        }
        checkEq(cols, 10, "getColumns count for jdbc_types");
        check(sawI64, "getColumns lists i64");

        boolean sawPk = false;
        try (ResultSet rs = md.getPrimaryKeys(null, null, "jdbc_types")) {
            while (rs.next()) {
                if ("id".equals(rs.getString("COLUMN_NAME"))) sawPk = true;
            }
        }
        check(sawPk, "getPrimaryKeys finds id");

        // ResultSetMetaData through Describe.
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT id, t, n FROM jdbc_types")) {
            ResultSetMetaData rmd = rs.getMetaData();
            checkEq(rmd.getColumnCount(), 3, "rsmd column count");
            checkEq(rmd.getColumnName(1), "id", "rsmd col 1 name");
            checkEq(rmd.getColumnType(1), Types.INTEGER, "rsmd col 1 type");
            checkEq(rmd.getColumnType(3), Types.NUMERIC, "rsmd col 3 type");
        }
        System.out.println("  metadata: ok");
    }

    // pgjdbc implements setQueryTimeout via the wire CancelRequest protocol.
    static void sectionCancel(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(2);
            long start = System.nanoTime();
            boolean threw = false;
            try {
                st.executeQuery(
                        "SELECT count(*) FROM generate_series(1, 3000) a, generate_series(1, 3000) b"
                                + " WHERE md5(md5(md5(md5(md5(a::text || '-' || b::text))))) LIKE '%aaaa%'");
            } catch (SQLException e) {
                threw = true;
                checkEq(e.getSQLState(), "57014", "cancel sqlstate");
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            check(threw, "query timeout threw");
            check(elapsedMs < 30_000, "cancel arrived promptly, took " + elapsedMs + "ms");
        }
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 41 + 1")) {
            check(rs.next() && rs.getInt(1) == 42, "connection usable after cancel");
        }
        System.out.println("  cancellation: ok");
    }
}
