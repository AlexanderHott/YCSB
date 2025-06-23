package site.ycsb.db;

import site.ycsb.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * YCSB binding for writing operations to a file via BufferedWriter.
 *
 * This class logs all database operations to an output text file.
 */
public class FileClient extends DB {

  private BufferedWriter writer;

  public static final String FILE_OUTPUT_PROPERTY_KEY = "file.output";
  public static final String FILE_OUTPUT_DEFAULT = "./workload.txt";

  @Override
  public void init() throws DBException {
    String filename = getProperties().getProperty(FILE_OUTPUT_PROPERTY_KEY, FILE_OUTPUT_DEFAULT);
    boolean append = true;
    try {
      writer = new BufferedWriter(new FileWriter(filename, append)); // append mode
    } catch (IOException e) {
      throw new DBException("Failed to open output file", e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (writer != null) {
        writer.flush();
        writer.close();
      }
    } catch (IOException e) {
      throw new DBException("Failed to close output file", e);
    }
  }

  private void writeLine(String line)  {
    try {
      writer.write(line);
      writer.newLine();
    } catch (IOException e) {
      System.out.println("Failed to write line: " + line);
    }
  }

  private String formatMap(Map<String, ByteIterator> map) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (Map.Entry<String, ByteIterator> entry : map.entrySet()) {
      sb.append(entry.getKey())
          .append("=")
          .append(entry.getValue().toString())
          .append(", ");
    }
    if (!map.isEmpty()) {
      sb.setLength(sb.length() - 2); // Remove trailing ", "
    }
    sb.append("}");
    return sb.toString();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    writeLine("P " + table + ":" + key + " " + fields);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    writeLine("I " + table + ":" + key + " " + formatMap(values));
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    writeLine("D " + table + ":" + key);
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    writeLine("U " + table + ":" + key + " " + formatMap(values));
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    writeLine("S " + table + ":" + startkey + " " + recordcount + " fields=" + fields);
    return Status.OK;
  }
}
