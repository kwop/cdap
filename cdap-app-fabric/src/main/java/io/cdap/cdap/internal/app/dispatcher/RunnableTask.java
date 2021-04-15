/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.dispatcher;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * RunnableTask represents a task that can be launched by a {@link TaskWorkerService}.
 */
public abstract class RunnableTask extends AbstractIdleService {
  public RunnableTask() {
  }

  protected abstract byte[] run(String param) throws Exception;

  public byte[] runTask(String param) throws Exception {
    return run(param);
  }
}
