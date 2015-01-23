/*
 * Copyright (C) 2015-2015 The Helenus Driver Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.helenusdriver.driver;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;

/**
 * The <code>RegularStatement</code> interface extends the functionality of
 * the {@link com.datastax.driver.core.RegularStatement} interface to make
 * them usable as part of this driver API.
 * <p>
 * <i>Note:</i> Note that although marked as batchable, the ability for the
 * statement to be batched depends solely on its content. only UPDATE, INSERT,
 * and DELETE statements are batchable.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface RegularStatement
  extends ResultSetStatement, BatchableStatement<ResultSet, ResultSetFuture> {}
