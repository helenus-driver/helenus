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
package org.helenus.junit.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * The <code>MethodRuleChain</code> class provides a JUnit4 method rule that
 * allows ordering of other method rules.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 24, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class MethodRuleChain implements MethodRule {
  /**
   * Holds an empty chain.
   *
   * @author paouelle
   */
  private static final MethodRuleChain EMPTY_CHAIN
    = new MethodRuleChain(Collections.emptyList());

  /**
   * Returns a <code>MethodRuleChain</code> without a {@link MethodRule}. This
   * method may be the starting point of a chain.
   *
   * @author paouelle
   *
   * @return a method rule chain without any method rules
   */
  public static MethodRuleChain emptyChain() {
    return MethodRuleChain.EMPTY_CHAIN;
  }

  /**
   * Returns a <code>MethodRuleChain</code> with a single {@link MethodRule}.
   * This method is the usual starting point of a chain.
   *
   * @author paouelle
   *
   * @param  outerRule the outer rule of the chain
   * @return a new chain with a single rule
   */
  public static MethodRuleChain outer(MethodRule outerRule) {
    return MethodRuleChain.emptyChain().around(outerRule);
  }

  /**
   * Holds the rules starting with the inner most.
   *
   * @author paouelle
   */
  private final List<MethodRule> rules;

  /**
   * Instantiates a new <code>MethodRuleChain</code> object.
   *
   * @author paouelle
   *
   * @param rules the non-<code>null</code> method rules
   */
  public MethodRuleChain(List<MethodRule> rules) {
    this.rules = rules;
  }

  /**
   * Create a new <code>MethodRuleChain</code>, which encloses the specified rule
   * with the rules of the current chain.
   *
   * @author paouelle
   *
   * @param  enclosedRule the rule to enclose
   * @return a new chain
   */
  public MethodRuleChain around(MethodRule enclosedRule) {
    final List<MethodRule> rules = new ArrayList<>();

    rules.add(enclosedRule);
    rules.addAll(this.rules);
    return new MethodRuleChain(rules);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.junit.rules.MethodRule#apply(org.junit.runners.model.Statement, org.junit.runners.model.FrameworkMethod, java.lang.Object)
   */
  @Override
  public Statement apply(Statement base, FrameworkMethod method, Object target) {
    for (final MethodRule r: rules) {
      base = r.apply(base, method, target);
    }
    return base;
  }
}
