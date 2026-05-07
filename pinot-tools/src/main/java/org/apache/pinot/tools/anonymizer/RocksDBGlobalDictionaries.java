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

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;


/**
 * RocksDB-backed implementation of {@link GlobalDictionaries}. Spills the per-column
 * (origValue -> derivedValue) mapping to disk so that distinct values from very large
 * (hundreds of millions to billions of rows) Pinot tables don't have to fit in JVM heap.
 *
 * Same algorithm as {@link MapBasedGlobalDictionaries}, same on-disk {@code .dict} output.
 * Keys are encoded type-stably so RocksDB's byte-wise iteration order matches the natural
 * sort order of the original values.
 */
public class RocksDBGlobalDictionaries implements GlobalDictionaries, Closeable {
  static {
    RocksDB.loadLibrary();
  }

  private static final int INT_BASE_VALUE = 1000;
  private static final long LONG_BASE_VALUE = 100000;
  private static final float FLOAT_BASE_VALUE = 100.23f;
  private static final double DOUBLE_BASE_VALUE = 1000.2375;

  private final Path _dbDir;
  private final RocksDB _db;
  private final Options _dbOptions;
  private final WriteOptions _writeOptions;
  private final boolean _ownsDir;
  private final Map<String, ColumnInfo> _columns = new HashMap<>();
  private short _nextColumnId = 0;
  private final Random _stringRandom;

  RocksDBGlobalDictionaries() {
    this(createTempDir(), true, null);
  }

  RocksDBGlobalDictionaries(Path dbDir, boolean ownsDir, Long stringSeed) {
    _dbDir = dbDir;
    _ownsDir = ownsDir;
    _dbOptions = new Options().setCreateIfMissing(true);
    _writeOptions = new WriteOptions().setDisableWAL(true);
    _stringRandom = stringSeed == null ? null : new Random(stringSeed);
    try {
      Files.createDirectories(_dbDir);
      _db = RocksDB.open(_dbOptions, _dbDir.toString());
    } catch (Exception e) {
      throw new RuntimeException("Failed to open RocksDB at " + _dbDir, e);
    }
  }

  private static Path createTempDir() {
    try {
      return Files.createTempDirectory("pinot-anonymizer-gd-");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnInfo getOrCreateColumn(String column, FieldSpec.DataType dataType) {
    ColumnInfo info = _columns.get(column);
    if (info == null) {
      info = new ColumnInfo(_nextColumnId++, dataType);
      _columns.put(column, info);
    }
    return info;
  }

  @Override
  public void addOrigValueToGlobalDictionary(Object origValue, String column, ColumnMetadata columnMetadata,
      int cardinality) {
    ColumnInfo info = getOrCreateColumn(column, columnMetadata.getDataType());
    byte[] key = makeKey(info, origValue);
    try {
      _db.put(_writeOptions, key, EMPTY);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void sortOriginalValuesInGlobalDictionaries() {
    // RocksDB iteration order over the encoded keys matches sorted original-value order.
  }

  @Override
  public void addDerivedValuesToGlobalDictionaries() {
    for (Map.Entry<String, ColumnInfo> entry : _columns.entrySet()) {
      ColumnInfo info = entry.getValue();
      switch (info._dataType) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          assignNumericDerived(info);
          break;
        case STRING:
          assignStringDerived(info);
          break;
        case BYTES:
          assignBytesDerived(info);
          break;
        default:
          throw new UnsupportedOperationException(
              "global dictionary currently does not support: " + info._dataType.name());
      }
    }
  }

  private void assignNumericDerived(ColumnInfo info) {
    try (WriteBatch batch = new WriteBatch();
        RocksIterator it = newPrefixIterator(info)) {
      byte[] prefix = makePrefix(info._id);
      it.seek(prefix);
      int i = 0;
      while (it.isValid() && hasPrefix(it.key(), prefix)) {
        byte[] derived = encodeNumericDerived(info._dataType, i++);
        batch.put(it.key(), derived);
        if (i % 100_000 == 0) {
          _db.write(_writeOptions, batch);
          batch.clear();
        }
        it.next();
      }
      _db.write(_writeOptions, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] encodeNumericDerived(FieldSpec.DataType type, int i) {
    switch (type) {
      case INT:
        return ByteBuffer.allocate(4).putInt(INT_BASE_VALUE + i).array();
      case LONG:
        return ByteBuffer.allocate(8).putLong(LONG_BASE_VALUE + i).array();
      case FLOAT:
        return ByteBuffer.allocate(4).putFloat(FLOAT_BASE_VALUE + i).array();
      case DOUBLE:
        return ByteBuffer.allocate(8).putDouble(DOUBLE_BASE_VALUE + i).array();
      default:
        throw new IllegalStateException();
    }
  }

  private void assignStringDerived(ColumnInfo info) {
    byte[] prefix = makePrefix(info._id);
    List<String> generated = new ArrayList<>();
    List<byte[]> keys = new ArrayList<>();
    try (RocksIterator it = newPrefixIterator(info)) {
      it.seek(prefix);
      while (it.isValid() && hasPrefix(it.key(), prefix)) {
        byte[] keyCopy = it.key();
        keys.add(keyCopy);
        String orig = new String(keyCopy, prefix.length, keyCopy.length - prefix.length, StandardCharsets.UTF_8);
        if (orig.isEmpty() || orig.equals(" ") || orig.equals("null")) {
          generated.add("null");
        } else {
          generated.add(_stringRandom == null ? RandomStringUtils.randomAlphanumeric(orig.length())
              : RandomStringUtils.random(orig.length(), 0, 0, true, true, null, _stringRandom));
        }
        it.next();
      }
    }
    Collections.sort(generated);
    try (WriteBatch batch = new WriteBatch()) {
      for (int i = 0; i < keys.size(); i++) {
        batch.put(keys.get(i), generated.get(i).getBytes(StandardCharsets.UTF_8));
        if ((i + 1) % 100_000 == 0) {
          _db.write(_writeOptions, batch);
          batch.clear();
        }
      }
      _db.write(_writeOptions, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private void assignBytesDerived(ColumnInfo info) {
    byte[] prefix = makePrefix(info._id);
    List<ByteArray> generated = new ArrayList<>();
    List<byte[]> keys = new ArrayList<>();
    Random random = _stringRandom == null ? new Random() : _stringRandom;
    try (RocksIterator it = newPrefixIterator(info)) {
      it.seek(prefix);
      while (it.isValid() && hasPrefix(it.key(), prefix)) {
        byte[] keyCopy = it.key();
        keys.add(keyCopy);
        int origLen = keyCopy.length - prefix.length;
        if (origLen == 0) {
          generated.add(new ByteArray(new byte[0]));
        } else {
          byte[] g = new byte[origLen];
          random.nextBytes(g);
          generated.add(new ByteArray(g));
        }
        it.next();
      }
    }
    Collections.sort(generated);
    try (WriteBatch batch = new WriteBatch()) {
      for (int i = 0; i < keys.size(); i++) {
        batch.put(keys.get(i), generated.get(i).getBytes());
        if ((i + 1) % 100_000 == 0) {
          _db.write(_writeOptions, batch);
          batch.clear();
        }
      }
      _db.write(_writeOptions, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void serialize(String outputDir)
      throws Exception {
    for (Map.Entry<String, ColumnInfo> entry : _columns.entrySet()) {
      String column = entry.getKey();
      ColumnInfo info = entry.getValue();
      byte[] prefix = makePrefix(info._id);
      try (PrintWriter writer = new PrintWriter(
          new BufferedWriter(new FileWriter(outputDir + "/" + column + DICT_FILE_EXTENSION)));
          RocksIterator it = newPrefixIterator(info)) {
        it.seek(prefix);
        while (it.isValid() && hasPrefix(it.key(), prefix)) {
          byte[] key = it.key();
          byte[] val = it.value();
          writer.println(decodeOrig(info._dataType, key, prefix.length));
          writer.println(decodeDerived(info._dataType, val));
          it.next();
        }
        writer.flush();
      }
    }
  }

  @Override
  public Object getDerivedValueForOrigValueSV(String column, Object origValue) {
    ColumnInfo info = _columns.get(column);
    byte[] key = makeKey(info, origValue);
    try {
      byte[] val = _db.get(key);
      if (val == null) {
        return null;
      }
      return decodeDerivedObject(info._dataType, val);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object[] getDerivedValuesForOrigValuesMV(String column, Object[] origValues) {
    Object[] out = new Object[origValues.length];
    for (int i = 0; i < origValues.length; i++) {
      out[i] = getDerivedValueForOrigValueSV(column, origValues[i]);
    }
    return out;
  }

  private Object decodeDerivedObject(FieldSpec.DataType type, byte[] val) {
    switch (type) {
      case INT:
        return ByteBuffer.wrap(val).getInt();
      case LONG:
        return ByteBuffer.wrap(val).getLong();
      case FLOAT:
        return ByteBuffer.wrap(val).getFloat();
      case DOUBLE:
        return ByteBuffer.wrap(val).getDouble();
      case STRING:
        return new String(val, StandardCharsets.UTF_8);
      case BYTES:
        return new ByteArray(val);
      default:
        throw new IllegalStateException();
    }
  }

  private String decodeOrig(FieldSpec.DataType type, byte[] key, int offset) {
    int len = key.length - offset;
    ByteBuffer buf = ByteBuffer.wrap(key, offset, len);
    switch (type) {
      case INT: {
        int v = buf.getInt() ^ 0x80000000;
        return Integer.toString(v);
      }
      case LONG: {
        long v = buf.getLong() ^ 0x8000000000000000L;
        return Long.toString(v);
      }
      case FLOAT: {
        int bits = buf.getInt();
        if ((bits & 0x80000000) != 0) {
          bits ^= 0x80000000;
        } else {
          bits = ~bits;
        }
        return Float.toString(Float.intBitsToFloat(bits));
      }
      case DOUBLE: {
        long bits = buf.getLong();
        if ((bits & 0x8000000000000000L) != 0) {
          bits ^= 0x8000000000000000L;
        } else {
          bits = ~bits;
        }
        return Double.toString(Double.longBitsToDouble(bits));
      }
      case STRING:
        return new String(key, offset, len, StandardCharsets.UTF_8);
      case BYTES: {
        byte[] copy = new byte[len];
        System.arraycopy(key, offset, copy, 0, len);
        return new ByteArray(copy).toString();
      }
      default:
        throw new IllegalStateException();
    }
  }

  private String decodeDerived(FieldSpec.DataType type, byte[] val) {
    switch (type) {
      case INT:
        return Integer.toString(ByteBuffer.wrap(val).getInt());
      case LONG:
        return Long.toString(ByteBuffer.wrap(val).getLong());
      case FLOAT:
        return Float.toString(ByteBuffer.wrap(val).getFloat());
      case DOUBLE:
        return Double.toString(ByteBuffer.wrap(val).getDouble());
      case STRING:
        return new String(val, StandardCharsets.UTF_8);
      case BYTES:
        return new ByteArray(val).toString();
      default:
        throw new IllegalStateException();
    }
  }

  private byte[] makeKey(ColumnInfo info, Object origValue) {
    byte[] encoded = encodeOrig(info._dataType, origValue);
    byte[] key = new byte[2 + encoded.length];
    key[0] = (byte) (info._id >>> 8);
    key[1] = (byte) info._id;
    System.arraycopy(encoded, 0, key, 2, encoded.length);
    return key;
  }

  private byte[] makePrefix(short id) {
    return new byte[]{(byte) (id >>> 8), (byte) id};
  }

  private byte[] encodeOrig(FieldSpec.DataType type, Object value) {
    switch (type) {
      case INT:
        return ByteBuffer.allocate(4).putInt(((Integer) value) ^ 0x80000000).array();
      case LONG:
        return ByteBuffer.allocate(8).putLong(((Long) value) ^ 0x8000000000000000L).array();
      case FLOAT: {
        int bits = Float.floatToRawIntBits((Float) value);
        bits = (bits & 0x80000000) == 0 ? bits ^ 0x80000000 : ~bits;
        return ByteBuffer.allocate(4).putInt(bits).array();
      }
      case DOUBLE: {
        long bits = Double.doubleToRawLongBits((Double) value);
        bits = (bits & 0x8000000000000000L) == 0 ? bits ^ 0x8000000000000000L : ~bits;
        return ByteBuffer.allocate(8).putLong(bits).array();
      }
      case STRING:
        return ((String) value).getBytes(StandardCharsets.UTF_8);
      case BYTES:
        if (value instanceof ByteArray) {
          return ((ByteArray) value).getBytes();
        }
        return (byte[]) value;
      default:
        throw new UnsupportedOperationException("unsupported data type: " + type);
    }
  }

  private RocksIterator newPrefixIterator(ColumnInfo info) {
    byte[] prefix = makePrefix(info._id);
    short nextId = (short) (info._id + 1);
    byte[] upper = new byte[]{(byte) (nextId >>> 8), (byte) nextId};
    ReadOptions opts = new ReadOptions().setIterateUpperBound(new Slice(upper));
    RocksIterator it = _db.newIterator(opts);
    // Note: closing the iterator's ReadOptions/Slice early is fine; iterator copies what it needs.
    return it;
  }

  private static boolean hasPrefix(byte[] key, byte[] prefix) {
    if (key.length < prefix.length) {
      return false;
    }
    for (int i = 0; i < prefix.length; i++) {
      if (key[i] != prefix[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (_db != null) {
      _db.close();
    }
    if (_writeOptions != null) {
      _writeOptions.close();
    }
    if (_dbOptions != null) {
      _dbOptions.close();
    }
    if (_ownsDir) {
      deleteDir(_dbDir.toFile());
    }
  }

  private static void deleteDir(File f) {
    if (f == null || !f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      File[] children = f.listFiles();
      if (children != null) {
        for (File c : children) {
          deleteDir(c);
        }
      }
    }
    f.delete();
  }

  private static final byte[] EMPTY = new byte[0];

  private static class ColumnInfo {
    final short _id;
    final FieldSpec.DataType _dataType;

    ColumnInfo(short id, FieldSpec.DataType dataType) {
      _id = id;
      _dataType = dataType;
    }
  }
}
