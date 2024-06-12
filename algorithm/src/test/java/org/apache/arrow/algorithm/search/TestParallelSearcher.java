/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.algorithm.search;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test cases for {@link ParallelSearcher}. */
public class TestParallelSearcher {

  private enum ComparatorType {
    EqualityComparator,
    OrderingComparator;
  }

  private static final int VECTOR_LENGTH = 10000;

  private BufferAllocator allocator;

  private ExecutorService threadPool;

  public static Stream<Arguments> getComparatorName() {
    List<Arguments> params = new ArrayList<>();
    int[] threadCounts = {1, 2, 5, 10, 20, 50};
    for (ComparatorType type : ComparatorType.values()) {
      for (int count : threadCounts) {
        params.add(Arguments.of(type, count));
      }
    }
    return params.stream();
  }

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
    if (threadPool != null) {
      threadPool.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("getComparatorName")
  public void testParallelIntSearch(ComparatorType comparatorType, int threadCount)
      throws ExecutionException, InterruptedException {
    threadPool = Executors.newFixedThreadPool(threadCount);
    try (IntVector targetVector = new IntVector("targetVector", allocator);
        IntVector keyVector = new IntVector("keyVector", allocator)) {
      targetVector.allocateNew(VECTOR_LENGTH);
      keyVector.allocateNew(VECTOR_LENGTH);

      // if we are comparing elements using equality semantics, we do not need a comparator here.
      VectorValueComparator<IntVector> comparator =
          comparatorType == ComparatorType.EqualityComparator
              ? null
              : DefaultVectorComparators.createDefaultComparator(targetVector);

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.set(i, i);
        keyVector.set(i, i * 2);
      }
      targetVector.setValueCount(VECTOR_LENGTH);
      keyVector.setValueCount(VECTOR_LENGTH);

      ParallelSearcher<IntVector> searcher =
          new ParallelSearcher<>(targetVector, threadPool, threadCount);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int pos =
            comparator == null
                ? searcher.search(keyVector, i)
                : searcher.search(keyVector, i, comparator);
        if (i * 2 < VECTOR_LENGTH) {
          assertEquals(i * 2, pos);
        } else {
          assertEquals(-1, pos);
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getComparatorName")
  public void testParallelStringSearch(ComparatorType comparatorType, int threadCount)
      throws ExecutionException, InterruptedException {
    threadPool = Executors.newFixedThreadPool(threadCount);
    try (VarCharVector targetVector = new VarCharVector("targetVector", allocator);
        VarCharVector keyVector = new VarCharVector("keyVector", allocator)) {
      targetVector.allocateNew(VECTOR_LENGTH);
      keyVector.allocateNew(VECTOR_LENGTH);

      // if we are comparing elements using equality semantics, we do not need a comparator here.
      VectorValueComparator<VarCharVector> comparator =
          comparatorType == ComparatorType.EqualityComparator
              ? null
              : DefaultVectorComparators.createDefaultComparator(targetVector);

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        targetVector.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        keyVector.setSafe(i, String.valueOf(i * 2).getBytes(StandardCharsets.UTF_8));
      }
      targetVector.setValueCount(VECTOR_LENGTH);
      keyVector.setValueCount(VECTOR_LENGTH);

      ParallelSearcher<VarCharVector> searcher =
          new ParallelSearcher<>(targetVector, threadPool, threadCount);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int pos =
            comparator == null
                ? searcher.search(keyVector, i)
                : searcher.search(keyVector, i, comparator);
        if (i * 2 < VECTOR_LENGTH) {
          assertEquals(i * 2, pos);
        } else {
          assertEquals(-1, pos);
        }
      }
    }
  }
}
