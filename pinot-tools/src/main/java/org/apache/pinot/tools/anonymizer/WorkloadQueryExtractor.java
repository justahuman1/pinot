/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.anonymizer;

import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.Separators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Java port of {@code stress-testing/scripts/extract-prod-queries.py}.
 *
 * Reads a query-mix.sh .out file, finds {@code Q2_sample[_d<N>_<table_id>]}
 * blocks, parses the JSON payload after the {@code ----- OUTPUT -----} marker,
 * and emits {@code queries.txt} (distinct queries) and {@code queries.json}
 * (per-query counts/fanouts/statuses) into {@code outDir}.
 *
 * Output is byte-identical to the Python reference on the same input, so a
 * simple {@code diff} against the Python output is a meaningful parity check.
 */
public class WorkloadQueryExtractor {

  private static final Pattern BLOCK_RE = Pattern.compile(
      "===== (Q2_sample(?:_d(\\d+)_(\\S+))?) =====.*?\\n----- OUTPUT -----\\n(.*?)\\n===== END \\1 =====",
      Pattern.DOTALL);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private WorkloadQueryExtractor() {
  }

  /**
   * Reads {@code inFile}, optionally filters to {@code tableId}, and writes
   * {@code queries.txt} + {@code queries.json} under {@code outDir}.
   *
   * @param inFile path to a query-mix.sh .out file
   * @param tableId if non-null, only blocks whose {@code <table_id>} capture
   *     group equals this string are kept; blocks without a {@code <table_id>}
   *     (older format) pass through unfiltered
   * @param outDir output directory (created if missing)
   */
  public static void run(String inFile, String tableId, String outDir)
      throws IOException {
    String text = new String(Files.readAllBytes(Paths.get(inFile)), StandardCharsets.UTF_8);

    // counts[query] -> count
    Map<String, Long> counts = new LinkedHashMap<>();
    // fanouts[query][fanoutValue] -> count
    Map<String, Map<String, Long>> fanouts = new LinkedHashMap<>();
    // statuses[query][statusValue] -> count
    Map<String, Map<String, Long>> statuses = new LinkedHashMap<>();

    long totalBlocks = 0;
    long totalRows = 0;
    long totalKept = 0;

    Matcher matcher = BLOCK_RE.matcher(text);
    while (matcher.find()) {
      totalBlocks++;
      String name = matcher.group(1);
      String tid = matcher.group(3);
      String body = matcher.group(4);

      // Mirrors Python: `if args.table and tid and tid != args.table: continue`
      // Blocks without a <table_id> capture (older format) pass through unfiltered.
      if (tableId != null && tid != null && !tid.equals(tableId)) {
        continue;
      }

      List<Row> rows;
      try {
        rows = parseRows(body);
      } catch (Exception e) {
        System.err.println("WARN: failed to parse " + name + ": " + e);
        continue;
      }

      for (Row row : rows) {
        totalRows++;
        // Mirrors Python's filter: skip non-string queries, the sentinel
        // QUERY_TOO_LONG, and queries that are empty after stripping.
        if (row._query == null || "QUERY_TOO_LONG".equals(row._query)) {
          continue;
        }
        String q = row._query.trim();
        if (q.isEmpty()) {
          continue;
        }
        counts.merge(q, 1L, Long::sum);
        fanouts.computeIfAbsent(q, k -> new LinkedHashMap<>()).merge(row._fanout, 1L, Long::sum);
        statuses.computeIfAbsent(q, k -> new LinkedHashMap<>()).merge(row._status, 1L, Long::sum);
        totalKept++;
      }
    }

    File outDirFile = new File(outDir);
    if (!outDirFile.exists() && !outDirFile.mkdirs() && !outDirFile.isDirectory()) {
      throw new IOException("Failed to create output directory: " + outDir);
    }

    // Tie-break sort: by count desc, then by query text asc.
    List<Map.Entry<String, Long>> items = new ArrayList<>(counts.entrySet());
    items.sort(Comparator
        .comparingLong((Map.Entry<String, Long> e) -> e.getValue()).reversed()
        .thenComparing(Map.Entry::getKey));

    // Write queries.txt: queries joined by '\n', plus a trailing '\n'.
    // Mirrors Python's `"\n".join(...) + "\n"`, which yields a single '\n'
    // even when the list is empty.
    StringBuilder txt = new StringBuilder();
    for (int idx = 0; idx < items.size(); idx++) {
      if (idx > 0) {
        txt.append('\n');
      }
      txt.append(items.get(idx).getKey());
    }
    txt.append('\n');
    Path txtPath = outDirFile.toPath().resolve("queries.txt");
    Files.write(txtPath, txt.toString().getBytes(StandardCharsets.UTF_8));

    // Write queries.json: array of {count, fanouts, query, statuses}
    // pretty-printed with 2-space indent and all map keys sorted alphabetically.
    List<Map<String, Object>> jsonItems = new ArrayList<>(items.size());
    for (Map.Entry<String, Long> e : items) {
      Map<String, Object> obj = new LinkedHashMap<>();
      // The output map is rendered with sort_keys=True, so insertion order
      // here is irrelevant. Use TreeMap for the inner per-key maps so they're
      // sorted regardless of the writer's outer config.
      obj.put("count", e.getValue());
      obj.put("fanouts", new TreeMap<>(fanouts.get(e.getKey())));
      obj.put("query", e.getKey());
      obj.put("statuses", new TreeMap<>(statuses.get(e.getKey())));
      jsonItems.add(obj);
    }

    Path jsonPath = outDirFile.toPath().resolve("queries.json");
    Files.write(jsonPath, buildPrettyWriter().writeValueAsBytes(jsonItems));

    System.err.println(String.format("blocks=%d rows=%d kept=%d distinct=%d -> %s",
        totalBlocks, totalRows, totalKept, counts.size(), txtPath));
  }

  /**
   * Build an ObjectMapper writer that matches Python's
   * {@code json.dumps(indent=2, sort_keys=True)}: 2-space indent, LF newlines,
   * {@code ": "} between key and value, sorted map keys, ASCII-escape non-ASCII
   * code points using lowercase hex, and {@code []}/{@code {}} (no inner space)
   * for empty containers.
   */
  private static com.fasterxml.jackson.databind.ObjectWriter buildPrettyWriter() {
    ObjectMapper mapper = MAPPER.copy();
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    mapper.getFactory().enable(JsonWriteFeature.ESCAPE_NON_ASCII.mappedFeature());
    mapper.getFactory().disable(JsonWriteFeature.WRITE_HEX_UPPER_CASE.mappedFeature());

    Separators separators = Separators.createDefaultInstance()
        .withObjectFieldValueSpacing(Separators.Spacing.AFTER)
        .withObjectEmptySeparator("")
        .withArrayEmptySeparator("");
    DefaultPrettyPrinter pp = new DefaultPrettyPrinter()
        .withObjectIndenter(new DefaultIndenter("  ", DefaultIndenter.SYS_LF))
        .withArrayIndenter(new DefaultIndenter("  ", DefaultIndenter.SYS_LF))
        .withSeparators(separators);
    mapper.setDefaultPrettyPrinter(pp);
    return mapper.writerWithDefaultPrettyPrinter();
  }

  /**
   * Parse the JSON payload of a single Q2_sample block. Mirrors
   * {@code parse_rows} in the Python reference: take the first key of the
   * top-level object as the fabric key, then for each element pull its
   * {@code value} array and read the first value of each cell map (regardless
   * of the inner type key).
   *
   * Skips rows with fewer than 6 cells, missing/non-string queries, or queries
   * equal to {@code "QUERY_TOO_LONG"}.
   */
  private static List<Row> parseRows(String blob)
      throws IOException {
    JsonNode root = MAPPER.readTree(blob);
    Iterator<String> fields = root.fieldNames();
    if (!fields.hasNext()) {
      return new ArrayList<>();
    }
    JsonNode fabric = root.get(fields.next());
    JsonNode elements = fabric.get("elements");
    List<Row> rows = new ArrayList<>();
    if (elements == null || !elements.isArray()) {
      return rows;
    }

    for (JsonNode el : elements) {
      JsonNode value = el.get("value");
      // Mirrors Python's `if len(cells) < 6: continue` — rows with fewer than
      // 6 cells are dropped here and are NOT counted in `total_rows` later.
      if (value == null || !value.isArray() || value.size() < 6) {
        continue;
      }
      Row row = new Row();
      // Python keeps the row even when the query cell is missing or
      // non-string; the outer loop discards it later and still increments
      // total_rows. Mirror that: leave `_query` null when invalid.
      JsonNode queryCell = firstValue(value.get(0));
      if (queryCell != null && queryCell.isTextual()) {
        row._query = queryCell.asText();
      }
      row._fanout = stringifyCell(firstValue(value.get(4)));
      row._status = stringifyCell(firstValue(value.get(5)));
      rows.add(row);
    }
    return rows;
  }

  /**
   * Return the first value of a cell map (the {@code {"<type>": <cell>}}
   * wrapper produced by Espresso). Mirrors Python's
   * {@code next(iter(c.values()))}.
   */
  private static JsonNode firstValue(JsonNode cell) {
    if (cell == null || !cell.isObject()) {
      return null;
    }
    Iterator<JsonNode> values = cell.elements();
    return values.hasNext() ? values.next() : null;
  }

  /**
   * Render a cell value as a string for use as a Counter key. Mirrors
   * Python's behavior of using the unwrapped JSON value as a dict key (which
   * {@code json.dumps} would later stringify).
   */
  private static String stringifyCell(JsonNode node) {
    if (node == null || node.isNull()) {
      return "null";
    }
    if (node.isTextual()) {
      return node.asText();
    }
    return node.toString();
  }

  private static final class Row {
    String _query;
    String _fanout;
    String _status;
  }

  public static void main(String[] args)
      throws Exception {
    String inFile = null;
    String tableId = null;
    String outDir = null;
    int i = 0;
    while (i < args.length) {
      String arg = args[i];
      if (i + 1 >= args.length) {
        throw new IllegalArgumentException("Missing value for argument: " + arg);
      }
      String value = args[i + 1];
      switch (arg) {
        case "-in":
          inFile = value;
          break;
        case "-table":
          tableId = value;
          break;
        case "-outDir":
          outDir = value;
          break;
        default:
          throw new IllegalArgumentException("Unknown argument: " + arg);
      }
      i += 2;
    }
    if (inFile == null || outDir == null) {
      throw new IllegalArgumentException("Usage: WorkloadQueryExtractor -in <out-file> [-table <id>] -outDir <dir>");
    }
    run(inFile, tableId, outDir);
  }
}
