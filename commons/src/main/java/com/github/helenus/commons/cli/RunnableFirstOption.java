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
package com.github.helenus.commons.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * The <code>RunnableFirstOption</code> class extends Apache's {#link Option} class
 * in order to provide the option to embed the running logic inside the option
 * itself when it needs to be ran first before all other options.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class RunnableFirstOption extends Option {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -2924377096904713277L;

  /**
   * Instantiates a new <code>RunnableOption</code> object.
   *
   * @author paouelle
   *
   * @param  opt short representation of the option
   * @param  hasArg specifies whether the Option takes an argument or not
   * @param  description describes the function of the option
   * @throws IllegalArgumentException if there are any non valid
   *         Option characters in <code>opt</code>.
   */
  public RunnableFirstOption(String opt, boolean hasArg, String description)
    throws IllegalArgumentException {
    super(opt, hasArg, description);
  }

  /**
   * Instantiates a new <code>RunnableOption</code> object.
   *
   * @author paouelle
   *
   * @param  opt short representation of the option
   * @param  longOpt the long representation of the option
   * @param  hasArg specifies whether the Option takes an argument or not
   * @param  description describes the function of the option
   * @throws IllegalArgumentException if there are any non valid
   *         Option characters in <code>opt</code>.
   */
  public RunnableFirstOption(
    String opt,
    String longOpt,
    boolean hasArg,
    String description
  ) throws IllegalArgumentException {
    super(opt, longOpt, hasArg, description);
  }

  /**
   * Instantiates a new <code>RunnableOption</code> object.
   *
   * @author paouelle
   *
   * @param  opt short representation of the option
   * @param  description describes the function of the option
   * @throws IllegalArgumentException if there are any non valid
   *         Option characters in <code>opt</code>.
   */
  public RunnableFirstOption(String opt, String description)
    throws IllegalArgumentException {
    super(opt, description);
  }

  /**
   * Runs this particular option based on the provided command line.
   *
   * @author paouelle
   *
   * @param  line the command line for which to run this option
   * @throws Exception if an error occurred while running the command option
   */
  public abstract void run(CommandLine line) throws Exception;
}
