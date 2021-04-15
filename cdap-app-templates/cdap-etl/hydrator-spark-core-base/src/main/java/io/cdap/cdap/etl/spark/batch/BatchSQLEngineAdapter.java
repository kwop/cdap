/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.etl.spark.batch;

import com.google.common.base.Objects;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.SQLEngine;
import io.cdap.cdap.etl.api.engine.SQLOperationResult;
import io.cdap.cdap.etl.api.join.JoinDefinition;
import io.cdap.cdap.etl.engine.SQLEngineJob;
import io.cdap.cdap.etl.engine.SQLEngineJobType;
import io.cdap.cdap.etl.spark.SparkCollection;
import io.cdap.cdap.etl.spark.function.TransformFromPairFunction;
import io.cdap.cdap.etl.spark.function.TransformToPairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.currentThread;

/**
 * Adapter used to orchestrate interaction between the Pipeline Runner and the SQL Engine.
 * <p>
 * TODO: Add all operations supported by the SQL Engine.
 */
public class BatchSQLEngineAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSQLEngineAdapter.class);

  SQLEngine<?, ?, ?, ?> sqlEngine;
  ExecutorService executorService = Executors.newFixedThreadPool(10);
  PairFlatMapFunction<?, ?, ?> toPairFunction;
  FlatMapFunction<?, ?> fromPairFunction;
  Map<String, SQLEngineJob<?>> jobs;

  public BatchSQLEngineAdapter(SQLEngine<?, ?, ?, ?> sqlEngine) {
    this.sqlEngine = sqlEngine;
    this.toPairFunction = new TransformToPairFunction<>(sqlEngine.toKeyValue());
    this.fromPairFunction = new TransformFromPairFunction<>(sqlEngine.fromKeyValue());
    this.jobs = new ConcurrentHashMap<>();
  }

  /**
   * Creates a new job tu push a SparkCollection into the SQL engine.
   *
   * @param tableName  the name of the dataset to push
   * @param collection the Spark collection containing the dataset to push
   * @return Job representing this Push operation.
   */
  @SuppressWarnings("unchecked,raw")
  public SQLEngineJob<?> push(String tableName,
                              Schema schema,
                              SparkCollection<StructuredRecord> collection) {
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      OutputFormatProvider outputFormatProvider = sqlEngine.getPushProvider(tableName, schema);
      JavaPairRDD<?, ?> pairRdd =
        ((JavaRDD) collection.getUnderlying()).flatMapToPair(toPairFunction);
      RDDUtils.saveUsingOutputFormat(outputFormatProvider, pairRdd);
      return null;
    }, executorService);

    SQLEngineJob<Void> job = new SQLEngineJob<>(tableName, SQLEngineJobType.PUSH, future);
    jobs.put(tableName, job);

    return job;
  }

  /**
   * Creates a new job to pull a Spark Collection from the SQL engine
   *
   * @param tableName the name of the dataset to push
   * @param jsc       the Java Spark Context to use when mapping records.
   * @return Job representing this pull operation.
   */
  @SuppressWarnings("unchecked,raw")
  public SQLEngineJob<JavaRDD<StructuredRecord>> pull(String tableName,
                                                      Schema schema,
                                                      JavaSparkContext jsc) {
    CompletableFuture<JavaRDD<StructuredRecord>> future = CompletableFuture.supplyAsync(() -> {

      InputFormatProvider inputFormatProvider = sqlEngine.getPullProvider(tableName, schema);

      ClassLoader classLoader = Objects.firstNonNull(currentThread().getContextClassLoader(),
                                                     getClass().getClassLoader());

      JavaPairRDD pairRDD = RDDUtils.readUsingInputFormat(jsc, inputFormatProvider, classLoader, Object.class,
                                                          Object.class);

      return pairRDD.flatMap(fromPairFunction);
    });

    SQLEngineJob<JavaRDD<StructuredRecord>> job = new SQLEngineJob<>(tableName, SQLEngineJobType.PULL, future);
    jobs.put(tableName, job);

    return job;
  }

  /**
   * Executes a Join operation in the SQL engine
   * @param tableName the table name to use to store the result of the join operation
   * @param joinDefinition the Join Definition
   * @return Job representing this join operation
   */
  public SQLEngineJob<SQLOperationResult> join(String tableName, JoinDefinition joinDefinition) {
    if (!sqlEngine.canJoin(joinDefinition)) {
      throw new IllegalArgumentException("Unable to execute this join in the SQL engine");
    }

    CompletableFuture<SQLOperationResult> future = CompletableFuture.supplyAsync(() -> {
      return sqlEngine.join(tableName, joinDefinition);
    });

    SQLEngineJob<SQLOperationResult> job = new SQLEngineJob<>(tableName, SQLEngineJobType.JOIN, future);
    jobs.put(tableName, job);

    return job;
  }

  /**
   * Stops all jobs from executing and cleans up the SQL engine.
   */
  public void cancel() {
    executorService.shutdown();
    sqlEngine.cleanup(true);
  }
}
