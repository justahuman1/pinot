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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Workload filter extractor: Java port of {@code values-to-segments.py}.
 *
 * <p>Three subcommands:
 * <ul>
 *   <li>{@code extract} — parse a queries.txt and emit per-column JSON files of literal values
 *       and ranges seen in WHERE clauses.</li>
 *   <li>{@code map} — read per-column JSON and emit broker SQL queries that resolve which segments
 *       contain those values (TSV-formatted to stdout).</li>
 *   <li>{@code union} — read broker result JSON files and write the union of segment names.</li>
 * </ul>
 *
 * <p>Output JSON is byte-equivalent to the Python script on the same input (modulo predicate
 * shapes the Python {@code walk_predicates} skips). Keys at every level are sorted alphabetically
 * and indented with two spaces, matching {@code json.dumps(..., indent=2, sort_keys=True)}.
 */
public class WorkloadFilterExtractor {

  private static final Set<String> TIME_PARTITION_COLS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList("daysSinceEpoch", "messageTimeDays", "requestTimeDays")));

  private static final ObjectMapper SORTED_MAPPER =
      new ObjectMapper().enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);

  private WorkloadFilterExtractor() {
  }

  /**
   * Build a fresh DefaultPrettyPrinter that emits Python-style {@code ": "} separators and
   * 2-space indentation. {@link DefaultPrettyPrinter} holds mutable nesting state so callers
   * must construct a new instance per top-level write.
   */
  private static DefaultPrettyPrinter makePythonPrettyPrinter() {
    DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
    DefaultPrettyPrinter pp = new PythonStyleDefaultPrettyPrinter();
    pp.indentObjectsWith(indenter);
    pp.indentArraysWith(indenter);
    return pp;
  }

  private static final class PythonStyleDefaultPrettyPrinter extends DefaultPrettyPrinter {
    @Override
    public DefaultPrettyPrinter createInstance() {
      // Return a fresh instance so the printer's mutable nesting state doesn't leak across
      // top-level writes from the same ObjectWriter.
      return makePythonPrettyPrinter();
    }

    @Override
    public void writeObjectFieldValueSeparator(JsonGenerator g)
        throws IOException {
      // Python: ": " between key and value (no leading space).
      g.writeRaw(": ");
    }
  }

  /**
   * Serialize {@code value} to a JSON string matching the Python script's output:
   * keys sorted, two-space indent, empty containers as {@code []}/{@code \{\}} (no inner spaces).
   */
  private static String prettyJson(Object value)
      throws IOException {
    String json = SORTED_MAPPER.writer(makePythonPrettyPrinter()).writeValueAsString(value);
    // Jackson's DefaultPrettyPrinter renders empty containers as "[ ]" / "{ }". Python writes
    // "[]" / "{}". Replace those exact patterns; quoted-string content is unaffected because
    // Jackson always escapes embedded brackets/braces would not match this pattern (they sit
    // inside quoted strings without the surrounding-space framing).
    return json.replace("[ ]", "[]").replace("{ }", "{}");
  }

  // ============================================================
  // EXTRACT
  // ============================================================

  /**
   * Read a queries file (one SQL per line), parse each with Calcite, walk the WHERE clause and
   * collect equality literals, IN-list members, ranges (gt/gte/lt/lte/between), and LIKE
   * prefix patterns. Writes per-column JSON files plus _summary.json into outDir.
   */
  public static void runExtract(String queriesFile, String outDir)
      throws Exception {
    List<String> queries = readNonBlankLines(queriesFile);
    Map<String, Map<Object, Long>> eqValues = new HashMap<>();
    Map<String, List<Map<String, Object>>> ranges = new HashMap<>();
    int parseFailures = 0;

    for (String sql : queries) {
      PinotQuery q;
      try {
        q = CalciteSqlParser.compileToPinotQuery(sql);
      } catch (Throwable t) {
        parseFailures++;
        continue;
      }
      Expression where = q.getFilterExpression();
      if (where == null) {
        continue;
      }
      walkPredicates(where, eqValues, ranges);
    }

    File out = new File(outDir);
    if (!out.isDirectory() && !out.mkdirs() && !out.isDirectory()) {
      throw new IOException("Failed to create output directory: " + outDir);
    }

    Set<String> allCols = new TreeSet<>(eqValues.keySet());
    allCols.addAll(ranges.keySet());

    Map<String, Map<String, Integer>> summary = new TreeMap<>();
    for (String col : allCols) {
      Map<Object, Long> ctr = eqValues.getOrDefault(col, Collections.emptyMap());
      List<Map<String, Object>> rs = new ArrayList<>(ranges.getOrDefault(col, Collections.emptyList()));
      // Sort ranges by canonical (sorted-key) JSON string ascending
      rs.sort((a, b) -> {
        try {
          return SORTED_MAPPER.writeValueAsString(a).compareTo(SORTED_MAPPER.writeValueAsString(b));
        } catch (IOException e) {
          return 0;
        }
      });

      // Sort values by count desc, then String.valueOf(v) asc
      List<Map.Entry<Object, Long>> items = new ArrayList<>(ctr.entrySet());
      items.sort((a, b) -> {
        int c = Long.compare(b.getValue(), a.getValue());
        if (c != 0) {
          return c;
        }
        return String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey()));
      });

      List<Map<String, Object>> values = new ArrayList<>(items.size());
      for (Map.Entry<Object, Long> e : items) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("n", e.getValue().intValue());
        v.put("v", e.getKey());
        values.add(v);
      }

      Map<String, Object> doc = new LinkedHashMap<>();
      doc.put("column", col);
      doc.put("is_time_partition", TIME_PARTITION_COLS.contains(col));
      doc.put("n_distinct", ctr.size());
      doc.put("ranges", rs);
      doc.put("values", values);

      String json = prettyJson(doc);
      Files.write(Paths.get(outDir, col + ".json"), json.getBytes(StandardCharsets.UTF_8));

      Map<String, Integer> s = new LinkedHashMap<>();
      s.put("n_distinct", ctr.size());
      s.put("n_ranges", rs.size());
      summary.put(col, s);
    }

    String summaryJson = prettyJson(summary);
    Files.write(Paths.get(outDir, "_summary.json"), summaryJson.getBytes(StandardCharsets.UTF_8));

    System.err.println("queries=" + queries.size() + " parse_failures=" + parseFailures
        + " columns=" + summary.size() + " -> " + outDir);
  }

  private static List<String> readNonBlankLines(String path)
      throws IOException {
    List<String> lines = new ArrayList<>();
    try (BufferedReader r = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8)) {
      String line;
      while ((line = r.readLine()) != null) {
        String s = line.trim();
        if (!s.isEmpty()) {
          lines.add(s);
        }
      }
    }
    return lines;
  }

  /**
   * Walk a WHERE expression tree iteratively. AND/OR both recurse into operands and accumulate;
   * we treat OR as value-set union (no conjunctive context tracking).
   */
  private static void walkPredicates(Expression where, Map<String, Map<Object, Long>> eqValues,
      Map<String, List<Map<String, Object>>> ranges) {
    Deque<Expression> stack = new ArrayDeque<>();
    stack.push(where);
    while (!stack.isEmpty()) {
      Expression n = stack.pop();
      if (n == null || n.getType() != ExpressionType.FUNCTION) {
        continue;
      }
      Function fn = n.getFunctionCall();
      if (fn == null || fn.getOperator() == null) {
        continue;
      }
      String op = fn.getOperator().toUpperCase();
      List<Expression> ops = fn.getOperands();
      FilterKind kind;
      try {
        kind = FilterKind.valueOf(op);
      } catch (IllegalArgumentException e) {
        // function-in-WHERE we don't recognize: skip
        continue;
      }
      switch (kind) {
        case AND:
        case OR:
          if (ops != null) {
            for (Expression child : ops) {
              stack.push(child);
            }
          }
          break;
        case EQUALS: {
          if (ops == null || ops.size() != 2) {
            break;
          }
          String c = identifier(ops.get(0));
          Object v = literalValue(ops.get(1));
          if (c == null) {
            c = identifier(ops.get(1));
            v = literalValue(ops.get(0));
          }
          if (c != null && v != null) {
            eqValues.computeIfAbsent(c, k -> new HashMap<>()).merge(v, 1L, Long::sum);
          }
          break;
        }
        case IN: {
          if (ops == null || ops.size() < 2) {
            break;
          }
          String c = identifier(ops.get(0));
          if (c == null) {
            break;
          }
          Map<Object, Long> bucket = eqValues.computeIfAbsent(c, k -> new HashMap<>());
          for (int i = 1; i < ops.size(); i++) {
            Object v = literalValue(ops.get(i));
            if (v != null) {
              bucket.merge(v, 1L, Long::sum);
            }
          }
          break;
        }
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL: {
          if (ops == null || ops.size() != 2) {
            break;
          }
          String c = identifier(ops.get(0));
          Object v = literalValue(ops.get(1));
          if (c == null || v == null) {
            break;
          }
          Map<String, Object> r = new LinkedHashMap<>();
          r.put("op", rangeOpName(kind));
          r.put("value", v);
          ranges.computeIfAbsent(c, k -> new ArrayList<>()).add(r);
          break;
        }
        case BETWEEN: {
          if (ops == null || ops.size() != 3) {
            break;
          }
          String c = identifier(ops.get(0));
          Object lo = literalValue(ops.get(1));
          Object hi = literalValue(ops.get(2));
          if (c == null || lo == null || hi == null) {
            break;
          }
          Map<String, Object> r = new LinkedHashMap<>();
          r.put("op", "between");
          r.put("lo", lo);
          r.put("hi", hi);
          ranges.computeIfAbsent(c, k -> new ArrayList<>()).add(r);
          break;
        }
        case LIKE: {
          if (ops == null || ops.size() != 2) {
            break;
          }
          String c = identifier(ops.get(0));
          Object v = literalValue(ops.get(1));
          if (c == null || !(v instanceof String)) {
            break;
          }
          String pattern = (String) v;
          // Only single trailing % with no other wildcards — emit a like_prefix.
          if (!pattern.endsWith("%")) {
            break;
          }
          String prefix = pattern.substring(0, pattern.length() - 1);
          if (prefix.indexOf('%') >= 0 || prefix.indexOf('_') >= 0) {
            break;
          }
          Map<String, Object> r = new LinkedHashMap<>();
          r.put("op", "like_prefix");
          r.put("value", prefix);
          ranges.computeIfAbsent(c, k -> new ArrayList<>()).add(r);
          break;
        }
        default:
          // NOT, NOT_EQUALS, NOT_IN, REGEXP_LIKE, IS_NULL, IS_NOT_NULL, etc. — skip silently.
          break;
      }
    }
  }

  private static String rangeOpName(FilterKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return "gt";
      case GREATER_THAN_OR_EQUAL:
        return "gte";
      case LESS_THAN:
        return "lt";
      case LESS_THAN_OR_EQUAL:
        return "lte";
      default:
        throw new IllegalArgumentException("Not a range op: " + kind);
    }
  }

  private static String identifier(Expression e) {
    if (e == null || e.getType() != ExpressionType.IDENTIFIER) {
      return null;
    }
    Identifier id = e.getIdentifier();
    return id == null ? null : id.getName();
  }

  /**
   * Resolve a literal expression to a JSON-friendly value: Long/Integer/Double/Float/String/Boolean
   * or null. Returns null for non-literal or null literal.
   */
  private static Object literalValue(Expression e) {
    if (e == null || e.getType() != ExpressionType.LITERAL) {
      return null;
    }
    Literal lit = e.getLiteral();
    if (lit == null) {
      return null;
    }
    Object v = RequestUtils.getLiteralValue(lit);
    if (v == null) {
      return null;
    }
    // Normalize numeric types so JSON output matches the Python eq/in path:
    //   - Python writes ints as bare integers, floats as decimals.
    //   - Calcite typically gives us Integer/Long for ints, Double/Float for floats.
    // Pass through; Jackson serializes them accurately.
    return v;
  }

  // ============================================================
  // MAP
  // ============================================================

  /**
   * Read per-column JSON files from valuesDir and emit broker SQL TSV lines on stdout, one per
   * chunk. Skips time-partition columns (with stderr note) and columns with no values.
   *
   * <p>If {@code topN} is &gt; 0 and a column has more distinct values than {@code topN}, only the
   * first {@code topN} values (already sorted by count desc by {@code runExtract}) are kept before
   * chunking. Mitigates workload-aware sampling devolving to "pull everything" when filter
   * predicates span the table's history (long-tail URN sets).
   */
  public static void runMap(String valuesDir, String table, int inChunk, int topN)
      throws Exception {
    File dir = new File(valuesDir);
    File[] files = dir.listFiles();
    if (files == null) {
      throw new IOException("Not a directory: " + valuesDir);
    }
    List<File> jsonFiles = new ArrayList<>();
    for (File f : files) {
      String name = f.getName();
      if (name.endsWith(".json") && !name.startsWith("_")) {
        jsonFiles.add(f);
      }
    }
    jsonFiles.sort((a, b) -> a.getName().compareTo(b.getName()));

    int n = 0;
    for (File f : jsonFiles) {
      Map<String, Object> spec;
      try {
        spec = SORTED_MAPPER.readValue(f, java.util.Map.class);
      } catch (IOException e) {
        System.err.println("# skip unreadable " + f.getName() + ": " + e.getMessage());
        continue;
      }
      String col = (String) spec.get("column");
      Boolean isTp = (Boolean) spec.get("is_time_partition");
      if (col == null) {
        continue;
      }
      if (Boolean.TRUE.equals(isTp)) {
        System.err.println("# skip time-partition col " + col);
        continue;
      }
      Object valuesObj = spec.get("values");
      if (!(valuesObj instanceof List)) {
        continue;
      }
      List<?> raw = (List<?>) valuesObj;
      List<Object> values = new ArrayList<>(raw.size());
      for (Object item : raw) {
        if (item instanceof Map) {
          values.add(((Map<?, ?>) item).get("v"));
        }
      }
      if (values.isEmpty()) {
        continue;
      }
      if (topN > 0 && values.size() > topN) {
        System.err.println("# topN cap: " + col + " " + values.size() + " -> " + topN);
        values = new ArrayList<>(values.subList(0, topN));
      }
      for (int i = 0; i < values.size(); i += inChunk) {
        int end = Math.min(i + inChunk, values.size());
        StringBuilder inList = new StringBuilder();
        for (int j = i; j < end; j++) {
          if (j > i) {
            inList.append(",");
          }
          inList.append(quoteValue(values.get(j)));
        }
        String sql = "SELECT $segmentName, COUNT(*) FROM " + table
            + " WHERE \"" + col + "\" IN (" + inList + ") GROUP BY $segmentName LIMIT 1000000";
        System.out.println(col + "\t" + i + "\t" + sql);
        n++;
      }
    }
    System.err.println("emitted " + n + " chunked queries");
  }

  /**
   * Mirror Python {@code quote_value}: numerics emit unquoted, everything else single-quoted with
   * embedded single-quotes doubled. Booleans (and any non-numeric) are quoted as strings.
   */
  static String quoteValue(Object v) {
    if (v == null) {
      return "NULL";
    }
    if ((v instanceof Integer) || (v instanceof Long) || (v instanceof Short) || (v instanceof Byte)
        || (v instanceof Double) || (v instanceof Float)) {
      return String.valueOf(v);
    }
    String s = String.valueOf(v).replace("'", "''");
    return "'" + s + "'";
  }

  // ============================================================
  // UNION
  // ============================================================

  /**
   * Read broker result JSON files (one per fabric) and write the sorted union of segment names
   * (the first cell of each element's value array) to outFile. Trailing newline only when output
   * is non-empty (matches Python).
   *
   * <p>If {@code prefix} is non-null and non-empty, drop any segment name that doesn't start
   * with that prefix. Used to filter out cross-table $segmentName leaks from the broker on shared
   * servers (e.g., a query against ContentGestureAnalyticsChewy returning LinkClicks_CA_* segment
   * names because both tables share servers).
   */
  public static void runUnion(String outFile, List<String> resultJsons, String prefix)
      throws Exception {
    Set<String> segments = new TreeSet<>();
    int dropped = 0;
    for (String path : resultJsons) {
      Map<String, Object> obj;
      try {
        obj = SORTED_MAPPER.readValue(new File(path), java.util.Map.class);
      } catch (Exception e) {
        System.err.println("WARN: " + path + ": " + e.getMessage());
        continue;
      }
      if (obj.isEmpty()) {
        continue;
      }
      String fabricKey = obj.keySet().iterator().next();
      Object inner = obj.get(fabricKey);
      // pinot-tool segment list --json shape: {fabric: ["seg1", "seg2", ...]}.
      // Distinct from the broker-query shape we handle below.
      if (inner instanceof List) {
        for (Object el : (List<?>) inner) {
          if (!(el instanceof String)) {
            continue;
          }
          String name = (String) el;
          if (prefix != null && !prefix.isEmpty() && !name.startsWith(prefix)) {
            dropped++;
            continue;
          }
          segments.add(name);
        }
        continue;
      }
      if (!(inner instanceof Map)) {
        continue;
      }
      Object elementsObj = ((Map<?, ?>) inner).get("elements");
      if (!(elementsObj instanceof List)) {
        continue;
      }
      for (Object el : (List<?>) elementsObj) {
        if (!(el instanceof Map)) {
          continue;
        }
        Object valueObj = ((Map<?, ?>) el).get("value");
        if (!(valueObj instanceof List)) {
          continue;
        }
        List<?> cells = (List<?>) valueObj;
        if (cells.isEmpty()) {
          continue;
        }
        Object firstCell = cells.get(0);
        if (!(firstCell instanceof Map) || ((Map<?, ?>) firstCell).isEmpty()) {
          continue;
        }
        Object firstVal = ((Map<?, ?>) firstCell).values().iterator().next();
        if (firstVal == null) {
          continue;
        }
        String name = String.valueOf(firstVal);
        if (prefix != null && !prefix.isEmpty() && !name.startsWith(prefix)) {
          dropped++;
          continue;
        }
        segments.add(name);
      }
    }

    Path outPath = Paths.get(outFile);
    Path parent = outPath.getParent();
    if (parent != null && !Files.isDirectory(parent)) {
      Files.createDirectories(parent);
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String s : segments) {
      if (!first) {
        sb.append("\n");
      }
      sb.append(s);
      first = false;
    }
    if (!segments.isEmpty()) {
      sb.append("\n");
    }
    Files.write(outPath, sb.toString().getBytes(StandardCharsets.UTF_8));
    if (prefix != null && !prefix.isEmpty()) {
      System.err.println("unique_segments=" + segments.size() + " dropped_by_prefix=" + dropped
          + " (prefix=" + prefix + ") -> " + outFile);
    } else {
      System.err.println("unique_segments=" + segments.size() + " -> " + outFile);
    }
  }

  // ============================================================
  // CLI
  // ============================================================

  public static void main(String[] args)
      throws Exception {
    if (args.length == 0) {
      printUsageAndExit();
    }
    String sub = args[0];
    String[] rest = Arrays.copyOfRange(args, 1, args.length);
    switch (sub) {
      case "extract": {
        String queries = requireFlag(rest, "-queries", "--queries");
        String outDir = requireFlag(rest, "-outDir", "--out-dir");
        runExtract(queries, outDir);
        break;
      }
      case "map": {
        String valuesDir = requireFlag(rest, "-valuesDir", "--values-dir");
        String table = requireFlag(rest, "-table", "--table");
        String chunk = optionalFlag(rest, "-inChunk", "--in-chunk");
        int inChunk = chunk == null ? 1000 : Integer.parseInt(chunk);
        String topNStr = optionalFlag(rest, "-topN", "--top-n");
        int topN = topNStr == null ? 0 : Integer.parseInt(topNStr);
        runMap(valuesDir, table, inChunk, topN);
        break;
      }
      case "union": {
        String outFile = requireFlag(rest, "-out", "--out");
        String prefix = optionalFlag(rest, "-prefix", "--prefix");
        List<String> results = positionalArgs(rest);
        if (results.isEmpty()) {
          System.err.println("union requires at least one result-json positional arg");
          System.exit(2);
        }
        runUnion(outFile, results, prefix);
        break;
      }
      default:
        printUsageAndExit();
        break;
    }
  }

  private static String requireFlag(String[] args, String... names) {
    String v = optionalFlag(args, names);
    if (v == null) {
      System.err.println("missing required flag: " + names[0]);
      System.exit(2);
    }
    return v;
  }

  private static String optionalFlag(String[] args, String... names) {
    Set<String> aliases = new HashSet<>(Arrays.asList(names));
    for (int i = 0; i < args.length - 1; i++) {
      if (aliases.contains(args[i])) {
        return args[i + 1];
      }
    }
    return null;
  }

  private static List<String> positionalArgs(String[] args) {
    Set<String> known = new HashSet<>(Arrays.asList(
        "-queries", "--queries", "-outDir", "--out-dir",
        "-valuesDir", "--values-dir", "-table", "--table",
        "-inChunk", "--in-chunk", "-topN", "--top-n",
        "-out", "--out", "-prefix", "--prefix"));
    List<String> out = new ArrayList<>();
    int i = 0;
    while (i < args.length) {
      String a = args[i];
      if (known.contains(a)) {
        i += 2;
        continue;
      }
      if (a.startsWith("-")) {
        // unknown flag — skip with single advance to avoid eating positional.
        i++;
        continue;
      }
      out.add(a);
      i++;
    }
    return out;
  }

  private static void printUsageAndExit() {
    PrintStream e = System.err;
    e.println("usage: WorkloadFilterExtractor extract -queries <file> -outDir <dir>");
    e.println("       WorkloadFilterExtractor map -valuesDir <dir> -table <name> [-inChunk N] [-topN N]");
    e.println("       WorkloadFilterExtractor union -out <file> [-prefix <table-name>] <result-json> [<result-json> ...]");
    System.exit(2);
  }
}
