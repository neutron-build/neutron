import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.Status;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Stand-in "table" used only to record the offline YCSB fixtures of row
 * oltp-ycsb-pg17 (card NB-2) — see README.md beside this file.
 *
 * A file-backed key set whose insert rejects a duplicate key with
 * Status.ERROR, which is what the YCSB JDBC binding returns on
 * PostgreSQL's duplicate-key violation. Nothing here is a database: the
 * point is to make the real YCSB 0.17.0 client print its real report —
 * Return= and -FAILED lines included — without a server.
 */
public class StandinTable extends DB {
  private final Set<String> keys = new HashSet<>();
  private PrintWriter out;

  @Override
  public void init() {
    String file = System.getenv("STANDIN_FILE");
    if (file == null) {
      throw new RuntimeException("STANDIN_FILE not set");
    }
    try {
      Path path = Paths.get(file);
      if (Files.exists(path)) {
        for (String line : Files.readAllLines(path)) {
          if (!line.isEmpty()) {
            keys.add(line);
          }
        }
      }
      // append mode: a second `ycsb run` process sees the first one's keys
      out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    if (out != null) {
      out.close();
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    synchronized (keys) {
      if (!keys.add(key)) {
        return Status.ERROR;
      }
      out.println(key);
      out.flush();
    }
    return Status.OK;
  }

  // Reads, scans, updates and deletes always succeed: only the insert
  // collision is under test.

  @Override
  public Status read(
      String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.OK;
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }
}
