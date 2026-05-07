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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBGlobalDictionariesTest {

  private static ColumnMetadata mockMeta(FieldSpec.DataType type) {
    ColumnMetadata m = Mockito.mock(ColumnMetadata.class);
    Mockito.when(m.getDataType()).thenReturn(type);
    return m;
  }

  @Test
  public void testIntRoundTrip()
      throws Exception {
    Path tmp = Files.createTempDirectory("rocksgd-int-");
    try (RocksDBGlobalDictionaries gd = new RocksDBGlobalDictionaries(tmp.resolve("db"), true, null)) {
      ColumnMetadata meta = mockMeta(FieldSpec.DataType.INT);
      Random random = new Random(42);
      Set<Integer> distinct = new HashSet<>();
      while (distinct.size() < 10_000) {
        distinct.add(random.nextInt());
      }
      for (int v : distinct) {
        gd.addOrigValueToGlobalDictionary(v, "c1", meta, distinct.size());
      }
      gd.sortOriginalValuesInGlobalDictionaries();
      gd.addDerivedValuesToGlobalDictionaries();

      List<Integer> sorted = new ArrayList<>(distinct);
      Collections.sort(sorted);
      // Spot-check a few values for monotonic derived assignment.
      Object firstDerived = gd.getDerivedValueForOrigValueSV("c1", sorted.get(0));
      Object lastDerived = gd.getDerivedValueForOrigValueSV("c1", sorted.get(sorted.size() - 1));
      Assert.assertTrue(((Integer) lastDerived) > ((Integer) firstDerived));

      // Round-trip every value.
      for (int v : distinct) {
        Object d = gd.getDerivedValueForOrigValueSV("c1", v);
        Assert.assertNotNull(d, "missing derived for " + v);
        Assert.assertTrue(d instanceof Integer);
      }

      File outDir = tmp.resolve("out").toFile();
      Assert.assertTrue(outDir.mkdirs());
      gd.serialize(outDir.getAbsolutePath());
      File dictFile = new File(outDir, "c1.dict");
      Assert.assertTrue(dictFile.exists());
      List<String> lines = Files.readAllLines(dictFile.toPath());
      Assert.assertEquals(lines.size(), distinct.size() * 2);
    }
  }

  @Test
  public void testStringRoundTripAndSortedDerived()
      throws Exception {
    Path tmp = Files.createTempDirectory("rocksgd-str-");
    try (RocksDBGlobalDictionaries gd = new RocksDBGlobalDictionaries(tmp.resolve("db"), true, 7L)) {
      ColumnMetadata meta = mockMeta(FieldSpec.DataType.STRING);
      Random random = new Random(123);
      Set<String> distinct = new HashSet<>();
      while (distinct.size() < 1000) {
        int len = 4 + random.nextInt(12);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
          sb.append((char) ('a' + random.nextInt(26)));
        }
        distinct.add(sb.toString());
      }
      for (String s : distinct) {
        gd.addOrigValueToGlobalDictionary(s, "s1", meta, distinct.size());
      }
      gd.sortOriginalValuesInGlobalDictionaries();
      gd.addDerivedValuesToGlobalDictionaries();

      // Walk the originals in sort order, collect their derived values, assert sorted.
      List<String> sortedOrigs = new ArrayList<>(distinct);
      Collections.sort(sortedOrigs);
      List<String> derivedInOrigSortOrder = new ArrayList<>(sortedOrigs.size());
      for (String s : sortedOrigs) {
        Object d = gd.getDerivedValueForOrigValueSV("s1", s);
        Assert.assertNotNull(d);
        derivedInOrigSortOrder.add((String) d);
      }
      List<String> sortedDerived = new ArrayList<>(derivedInOrigSortOrder);
      Collections.sort(sortedDerived);
      Assert.assertEquals(derivedInOrigSortOrder, sortedDerived,
          "derived values must follow the same sort order as originals");
    }
  }

  @Test
  public void testReopenSurvives()
      throws Exception {
    Path tmp = Files.createTempDirectory("rocksgd-reopen-");
    Path dbDir = tmp.resolve("db");
    ColumnMetadata meta = mockMeta(FieldSpec.DataType.INT);
    try (RocksDBGlobalDictionaries gd = new RocksDBGlobalDictionaries(dbDir, false, null)) {
      for (int i = 0; i < 100; i++) {
        gd.addOrigValueToGlobalDictionary(i * 7, "k", meta, 100);
      }
      gd.addDerivedValuesToGlobalDictionaries();
    }
    // Reopen, keep prior data. Re-register column id, but values from the previous run
    // are present under id 0; we just verify the DB opens without panic and we can write
    // and serialize on a fresh column without error.
    try (RocksDBGlobalDictionaries gd2 = new RocksDBGlobalDictionaries(dbDir, true, null)) {
      gd2.addOrigValueToGlobalDictionary(42, "k2", meta, 1);
      gd2.addDerivedValuesToGlobalDictionaries();
      File outDir = tmp.resolve("out").toFile();
      Assert.assertTrue(outDir.mkdirs());
      gd2.serialize(outDir.getAbsolutePath());
      Assert.assertTrue(new File(outDir, "k2.dict").exists());
    }
  }

  @Test
  public void testDeterministicOutputForFixedSeed()
      throws Exception {
    byte[] firstRunBytes = runStringPipeline(99L);
    byte[] secondRunBytes = runStringPipeline(99L);
    Assert.assertEquals(firstRunBytes, secondRunBytes,
        "two runs at the same seed must produce byte-identical .dict output");
  }

  private byte[] runStringPipeline(long seed)
      throws Exception {
    Path tmp = Files.createTempDirectory("rocksgd-det-");
    try (RocksDBGlobalDictionaries gd = new RocksDBGlobalDictionaries(tmp.resolve("db"), true, seed)) {
      ColumnMetadata meta = mockMeta(FieldSpec.DataType.STRING);
      // Use a fixed input set so the run is fully deterministic.
      String[] inputs = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"};
      for (String s : inputs) {
        gd.addOrigValueToGlobalDictionary(s, "c", meta, inputs.length);
      }
      gd.sortOriginalValuesInGlobalDictionaries();
      gd.addDerivedValuesToGlobalDictionaries();
      File outDir = tmp.resolve("out").toFile();
      Assert.assertTrue(outDir.mkdirs());
      gd.serialize(outDir.getAbsolutePath());
      return Files.readAllBytes(new File(outDir, "c.dict").toPath());
    } finally {
      // Best effort cleanup.
      deleteRecursive(tmp.toFile());
    }
  }

  private static void deleteRecursive(File f) {
    if (f == null || !f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      File[] children = f.listFiles();
      if (children != null) {
        for (File c : children) {
          deleteRecursive(c);
        }
      }
    }
    f.delete();
  }
}
