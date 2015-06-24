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
package org.helenus.commons.collections;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.helenus.commons.collections.graph.ConcurrentHashDirectedGraph;
import org.helenus.commons.lang3.IllegalCycleException;

/**
 * The <code>GraphUtils</code> class provides utility functions for graphs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public class GraphUtils {
  /**
   * Recursively explores from the specified node, marking all nodes
   * encountered by the search.
   *
   * @author paouelle
   *
   * @param  node the node to begin the search from
   * @param  rg the reverse graph in which to perform the search
   * @param  order a list holding the topological sort of the graph
   * @param  visited a set of nodes that have already been visited
   * @param  expanding a set of nodes that being expanded in the order traversed
   * @param  reverse <code>true</code> to reverse the order of the sort
   * @param  omapper is a function used to map the object to a different one when
   *         building an exception message
   * @param  smapper is a function used to map the object to a string when
   *         building an exception message
   * @throws IllegalCycleException if the graph contains cycles
   */
  private static <T> void explore(
    DirectedGraph.Node<T> node,
    DirectedGraph<T> rg,
    LinkedList<T> order,
    Set<T> visited,
    Set<T> expanding,
    boolean reverse,
    Function<T, ?> omapper,
    Function<T, String> smapper
  ) {
    final T v = node.getValue();

    if (!visited.add(v)) { // we already done that node so nothing to do
      if (expanding.contains(v)) {
        // find that spot using an iterator where v is so we can print out
        // the cycle from that spot till the end, but since we have a reversed
        // graph, we need to reverse that cycle
        final LinkedList<T> cycle = new LinkedList<>();
        final Iterator<T> i = expanding.iterator();
        T t;

        while (!(t = i.next()).equals(v)) {}
        cycle.addFirst(t);
        while (i.hasNext()) {
          cycle.addFirst(i.next());
        }
        cycle.addFirst(v);
        final List<?> ocycle;

        if (omapper != null) {
          ocycle = cycle.stream().map(omapper).collect(Collectors.toList());
        } else {
          ocycle = cycle;
        }
        if (smapper == null) {
          throw new IllegalCycleException("cycle detected", ocycle);
        }
        throw new IllegalCycleException(
          "cycle detected" + cycle.stream()
            .map(smapper)
            .collect(Collectors.joining(" -> ", ": ", "")), ocycle
        );
      }
      return;
    }
    expanding.add(v);
    // recursively explore all of the node's predecessors
    node.edges().forEach(
      e -> explore(e, rg, order, visited, expanding, reverse, omapper, smapper)
    );
    // now that we have explored all predecessors, add the node to the order
    if (reverse) {
      order.addFirst(v);
    } else {
      order.add(v);
    }
    // mark that the node was fully expanded
    expanding.remove(v);
  }

  /**
   * Sorts the specified directed graph and obtains a topological sorting of
   * the nodes in the graph.
   *
   * @author paouelle
   *
   * @param  g the graph to be sorted
   * @param  reverse <code>true</code> to reverse the order of the sort
   * @param  omapper is a function used to map the object to a different one when
   *         building an exception message
   * @param  smapper is a function used to map the object to a string when
   *         building an exception message
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  private static <T> List<T> sort(
    DirectedGraph<T> g,
    boolean reverse,
    Function<T, ?> omapper,
    Function<T, String> smapper
  ) {
    // start with the reverse graph
    final DirectedGraph<T> rg = GraphUtils.reverse(g);
    // keep 3 structs - a set of visited nodes so that once we've added a node
    //                  to the list, we won't label it again
    //                - a list of nodes that actually holds the topological order
    //                - a set of all nodes that are currently being expanded
    // note: if the graph contains a cycle, then we can detect it if the node
    //       is already being expanded
    final int ssize = Math.max(8, rg.size() * 3 / 2);
    final LinkedList<T> order = new LinkedList<>();
    final Set<T> visited = new HashSet<>(ssize);
    final Set<T> expanding = new LinkedHashSet<>(ssize); // keep order

    rg.nodeSet().forEach(
      n -> GraphUtils.explore(
        n, rg, order, visited, expanding, reverse, omapper, smapper
      )
    );
    return order;
  }

  /**
   * Sorts the specified directed graph and obtains a reversed topological
   * sorting of the nodes in the graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to sort
   *
   * @param  g the graph to be sorted
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  public static <T> List<T> reverseSort(DirectedGraph<T> g) {
    return GraphUtils.sort(g, true, null, null);
  }

  /**
   * Sorts the specified directed graph and obtains a topological sorting of
   * the nodes in the graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to sort
   *
   * @param  g the graph to be sorted
   * @param  smapper is a function used to map the object to a string when
   *         building an exception message
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  public static <T> List<T> reverseSort(
    DirectedGraph<T> g, Function<T, String> smapper
  ) {
    return GraphUtils.sort(g, true, null, smapper);
  }

  /**
   * Sorts the specified directed graph and obtains a topological sorting of
   * the nodes in the graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to sort
   *
   * @param  g the graph to be sorted
   * @param  omapper is a function used to map the object to a different one when
   *         building an exception message
   * @param  smapper is a function used to map the object to a string when
   *         building an exception message
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  public static <T> List<T> reverseSort(
    DirectedGraph<T> g, Function<T, ?> omapper, Function<T, String> smapper
  ) {
    return GraphUtils.sort(g, true, omapper, smapper);
  }

  /**
   * Sorts the specified directed graph and obtains a topological sorting of
   * the nodes in the graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to sort
   *
   * @param  g the graph to be sorted
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  public static <T> List<T> sort(DirectedGraph<T> g) {
    return GraphUtils.sort(g, false, null, null);
  }

  /**
   * Sorts the specified directed graph and obtains a topological sorting of
   * the nodes in the graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to sort
   *
   * @param  g the graph to be sorted
   * @param  smapper is a function used to map the object to a string when
   *         building an exception message
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  public static <T> List<T> sort(
    DirectedGraph<T> g, Function<T, String> smapper
  ) {
    return GraphUtils.sort(g, false, null, smapper);
  }

  /**
   * Sorts the specified directed graph and obtains a topological sorting of
   * the nodes in the graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to sort
   *
   * @param  g the graph to be sorted
   * @param  omapper is a function used to map the object to a different one when
   *         building an exception message
   * @param  smapper is a function used to map the object to a string when
   *         building an exception message
   * @return a topological sort of the graph
   * @throws IllegalCycleException if the graph contains cycles
   */
  public static <T> List<T> sort(
    DirectedGraph<T> g, Function<T, ?> omapper, Function<T, String> smapper
  ) {
    return GraphUtils.sort(g, false, omapper, smapper);
  }

  /**
   * Gets the reverse of the input graph.
   *
   * @author paouelle
   *
   * @param <T> the type of the graph to reverse
   *
   * @param  g a graph to reverse
   * @return the reverse of that graph
   */
  public static <T> DirectedGraph<T> reverse(DirectedGraph<T> g) {
    final DirectedGraph<T> result = new ConcurrentHashDirectedGraph<>(g.size());

    // add all the nodes from the original graph
    result.addAll(g);
    // scan over all the edges in the graph, adding their reverse to the new graph
    g.nodeSet().forEach(
      n -> n.edges()
        .forEach(
          e -> result.addEdge(e.getValue(), n.getValue())
        )
    );
    return result;
  }

  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private GraphUtils() {}
}
