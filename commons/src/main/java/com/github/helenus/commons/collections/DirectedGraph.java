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
package com.github.helenus.commons.collections;

import java.io.Serializable;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The <code>DirectedGraph</code> interface provides a collection of nodes with
 * outgoing edges to other nodes.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of elements in this graph
 *
 * @since 2.0
 */
public interface DirectedGraph<T> extends Set<T> {
  /**
   * Gets a node from this graph given its value.
   *
   * @author paouelle
   *
   * @param  val the value for the node to retrieve
   * @return the corresponding node from this graph or <code>null</code> if the
   *         graph doesn't contain a node with the specified value
   */
  public DirectedGraph.Node<T> get(T val);

  /**
   * Gets a {@link Set} view of all the nodes contained in this directed graph.
   * <p>
   * The set is backed by the graph, so changes to the graph are reflected in
   * the set, and vice-versa. If the graph is modified while an iteration over
   * the set is in progress (except through the iterator's own <code>remove()</code>
   * operation, or through the <code>addEdge()</code> or <code>removeEdge()</code>
   * operations on a graph node returned by the iterator) the results of the
   * iteration are undefined. The set supports node removal, which removes the
   * corresponding node from the graph, via the <code>Iterator.remove()<code>,
   * <code>Set.remove</code>, <code>removeAll</code>, <code>retainAll<code>,
   * and <code>clear</code> operations. It does not support the <code>add</code>
   * or <code>addAll</code> operations.
   *
   * @author paouelle
   *
   * @return a set view of the nodes contained in this graph
   */
  public Set<DirectedGraph.Node<T>> nodeSet();

  /**
   * Adds the specified nodes if they don't exist in the graph and build an arc
   * from the starting node to the destination one.
   *
   * @param start the starting node
   * @param dest the destination node
   */
  public void add(T start, T dest);

  /**
   * Adds an arc from the starting node to the destination one. If an arc
   * already exists, this operation is a no-op.
   *
   * @param  start the starting node
   * @param  dest the destination node
   * @throws NoSuchElementException if either nodes do not exist
   */
  public void addEdge(T start, T dest);

  /**
   * Removes the edge from the specified starting node to the specified destination
   * node from the graph.
   *
   * @param  start the starting node
   * @param  dest the destination node
   * @return <code>true</code> if an edge was removed; <code>false</code>
   *         otherwise
   * @throws NoSuchElementException if either nodes do not exist
   */
  public boolean removeEdge(T start, T dest);

  /**
   * Given two nodes in the graph, returns whether there is an edge from the
   * first node to the second node.
   *
   * @param  start the starting node
   * @param  dest the destination node
   * @return <code>true</code> if an edge exists from <code>start</code> to
   *         <code>dest</code>; <code>false</code>otherwise
   * @throws NoSuchElementException if either nodes do not exist
   */
  public boolean edgeExists(T start, T dest);

  /**
   * Given a node in the graph, returns a stream of the edges leaving that node.
   *
   * @param  node the node whose edges should be queried
   * @return a stream of the edges leaving that node
   * @throws NoSuchElementException if the node does not exist
   */
  public Stream<T> edgesFrom(T node);

  /**
   * Given a node in the graph, returns an immutable view of the edges
   * leaving that node as a set of endpoints.
   *
   * @param  node the node whose edges should be queried
   * @return an immutable view of the edges leaving that node
   * @throws NoSuchElementException if the node does not exist
   */
  public Set<T> getEdgesFrom(T node);

  /**
   * The <code>Node</code> interface represents a specific node in this directed
   * graph.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @param <T> the type of value
   *
   * @since 2.0
   */
  public interface Node<T> {
    /**
     * Gets a comparator that compares {@link DirectedGraph.Node} in natural
     * order on value.
     * <p>
     * The returned comparator is serializable and throws
     * {@link NullPointerException} when comparing a node with a null value.
     *
     * @param <T> the type of elements in this graph
     *
     * @return a comparator that compares {@link DirectedGraph.Node} in natural
     *         order on value
     *
     * @see Comparable
     */
    public static <T extends Comparable<? super T>> Comparator<DirectedGraph.Node<T>> comparingByValue() {
      return (Comparator<DirectedGraph.Node<T>> & Serializable)
              (c1, c2) -> c1.getValue().compareTo(c2.getValue());
    }

    /**
     * Gets a comparator that compares {@link DirectedGraph.Node} by value using
     * the given {@link Comparator}.
     * <p>
     * The returned comparator is serializable if the specified comparator is
     * also serializable.
     *
     * @param <T> the type of elements in this graph
     *
     * @param  cmp the value {@link Comparator}
     * @return a comparator that compares {@link DirectedGraph.Node} by the value
     */
    public static <T> Comparator<DirectedGraph.Node<T>> comparingByValue(Comparator<? super T> cmp) {
      org.apache.commons.lang3.Validate.notNull(cmp, "invalid null comparator");
      return (Comparator<DirectedGraph.Node<T>> & Serializable)
              (c1, c2) -> cmp.compare(c1.getValue(), c2.getValue());
    }

    /**
     * Gets the value for this node.
     *
     * @author paouelle
     *
     * @return this node's value
     */
    public T getValue();

    /**
     * Checks if there this node has an edge to another node.
     *
     * @param  dest the destination node
     * @return <code>true</code> if an edge exists from this node to
     *         <code>dest</code>; <code>false</code>otherwise
     * @throws NoSuchElementException if the destination node does not exist
     */
    public boolean edgeExists(T dest);

    /**
     * Gets an immutable view of the edge nodes leaving this node.
     *
     * @author paouelle
     *
     * @return an immutable view of the edge nodes leaving this node
     */
    public Set<Node<T>> getEdges();

    /**
     * Gets a stream of the edge nodes leaving this node.
     *
     * @author paouelle
     *
     * @return a stream of the edge nodes leaving this node
     */
    public Stream<Node<T>> edges();

    /**
     * Adds the specified destination node if missing and add an arc from this
     * node to the destination one.
     *
     * @param dest the destination node
     */
    public void add(T dest);

    /**
     * Adds an arc from this node to the destination one. If an arc
     * already exists, this operation is a no-op.
     *
     * @param  dest the destination node
     * @throws NoSuchElementException if the destination node does not exist
     */
    public void addEdge(T dest);

    /**
     * Removes the edge from this node to the specified destination node from
     * the graph.
     *
     * @param  dest the destination node
     * @return <code>true</code> if an edge was removed; <code>false</code>
     *         otherwise
     * @throws NoSuchElementException if the destination node does not exist
     */
    public boolean removeEdge(T dest);

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode();

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o);
  }
}
