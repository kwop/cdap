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

package io.cdap.cdap.etl.api.engine;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.KeyValueBiTransform;
import io.cdap.cdap.etl.api.PipelineConfigurable;
import io.cdap.cdap.etl.api.SubmitterLifecycle;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.join.JoinDefinition;

import java.util.concurrent.Future;

/**
 * A SQL Engine can be used to pushdown certain dataset operations.
 *
 * SQL Engines are implemented as plugins.
 *
 * Internally, the SQL Engine needs to handle retries and only surface {@link SQLEngineException} errors when there
 * is a problem that cannot be recovered from and pipeline must be stopped.
 *
 * @param <KEY_OUT> The type for the Output Key when mapping a StructuredRecord
 * @param <VALUE_OUT> The type for the Output Value when mapping a StructuredRecord
 * @param <KEY_IN> The type for the Input Key when building a StructuredRecord
 * @param <VALUE_IN> The type for the Input Value when building a StructuredRecord
 */
@Beta
public interface SQLEngine<KEY_OUT, VALUE_OUT, KEY_IN, VALUE_IN> extends PipelineConfigurable,
  SubmitterLifecycle<BatchContext>, KeyValueBiTransform<StructuredRecord, KEY_OUT, VALUE_OUT, KEY_IN, VALUE_IN> {
  String PLUGIN_TYPE = "sqlengine";

  /**
   * Creates an Output Format Provided that can be used to push records into a SQL Engine.
   *
   * After created, this table will be considered "locked" until the output has been committed.
   *
   * @param tableName The name of the table to use to store these records.
   * @return an {@link OutputFormatProvider} instance that can be used to write records to the SQL Engine into the
   * specified table.
   */
  OutputFormatProvider getPushProvider(String tableName) throws SQLEngineException;

  /**
   * Creates an InputFormatProvider that can be used to pull records from the specified table.
   * @param tableName the name of the table to pull records from
   * @return AN {@link InputFormatProvider} instance that can be used to read records form the specified table in the
   * SQL Engine.
   */
  InputFormatProvider getPullProvider(String tableName) throws SQLEngineException;

  /**
   * Check if this table exists in the SQL Engine.
   *
   * This is a blocking call. if the process to write records into a table is ongoing, this method will block until
   * the process completes. This ensures an accurate result for this operation.
   *
   * @param tableName the table name
   * @return boolean specifying if this table exists in the remote engine.
   */
  boolean exists(String tableName) throws SQLEngineException;

  /**
   * Check if the supplied Join Definition can be executed in this engine.
   * @param definition the join definition to check
   * @return boolean specifying if this join operation can be executed in the SQl Engine.
   */
  boolean canJoin(JoinDefinition definition);

  /**
   * Executes the join operation defined by the supplied join Definition and stores the result in the defined table.
   *
   * The returned {@link Future} can be used to determine the status of this task.
   *
   * @param tableName the name of the table to use to store results
   * @param definition the join definition
   * @return the {@link SQLOperationResult} instance with information about the execution of this task.
   */
  SQLOperationResult join(String tableName, JoinDefinition definition) throws SQLEngineException;

  /**
   * Deletes all temporary tables and cleans up all temporary data from the SQL engine
   * @param forceStop boolean specifying if all running tasks should be stopped at this time (if any are running).
   */
  void cleanup(boolean forceStop);
}
