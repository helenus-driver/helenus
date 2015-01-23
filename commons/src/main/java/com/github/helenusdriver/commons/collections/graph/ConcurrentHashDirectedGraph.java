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
package com.github.helenusdriver.commons.collections.graph;

import java.io.Serializable;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.helenusdriver.commons.collections.DirectedGraph;

/**
 * The <code>ConcurrentHashDirectedGraph</code> class provides an implementation
 * of the {@link DirectedGraph} interface that uses a {@link ConcurrentHashMap}
 * to keep track of the graph.
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
public class ConcurrentHashDirectedGraph<T>
  implements DirectedGraph<T>, Cloneable, Serializable {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -8430194904167408569L;

  /**
   * The <code>HashNode</code> class provides an implementation for a graph node.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @since 2.0
   */
  private class HashNode implements Node<T>, Cloneable, Serializable {
    /**
     * Holds the serialVersionUID.
     *
     * @author paouelle
     */
    private static final long serialVersionUID = 2377326682367937940L;

    /**
     * Holds the value for this node.
     *
     * @author paouelle
     */
    private final T value;

    /**
     * Holds the edges from this node.
     *
     * @author paouelle
     */
    @SuppressWarnings("synthetic-access")
    private final HashSet<HashNode> edges
      = new HashSet<>(Math.min(initialCapacity / 16, 8), loadFactor);

    /**
     * Instantiates a new <code>HashNode</code> object.
     *
     * @author paouelle
     *
     * @param value the value for this node.
     */
    HashNode(T value) {
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#getValue()
     */
    @Override
    public T getValue() {
      return value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#edgeExists(java.lang.Object)
     */
    @Override
    public boolean edgeExists(T dest) {
      @SuppressWarnings("synthetic-access")
      final HashNode node = graph.get(dest);

      if (node == null) {
        throw new NoSuchElementException("unknown destination node");
      }
      return edges.contains(node);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#getEdges()
     */
    @Override
    public Set<DirectedGraph.Node<T>> getEdges() {
      return Collections.unmodifiableSet(edges);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#edges()
     */
    @Override
    public Stream<DirectedGraph.Node<T>> edges() {
      return edges.stream().map(n -> n);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#add(java.lang.Object)
     */
    @Override
    public void add(T dest) {
      HashNode node = new HashNode(dest);
      @SuppressWarnings("synthetic-access")
      final HashNode old = graph.putIfAbsent(dest, node);

      if (old != null) { // we already had one so continue with it
        node = old;
      }
      edges.add(node);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#addEdge(java.lang.Object)
     */
    @Override
    public void addEdge(T dest) {
      @SuppressWarnings("synthetic-access")
      final HashNode node = graph.get(dest);

      if (node == null) {
        throw new NoSuchElementException("unknown destination node");
      }
      edges.add(node);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.commons.collections.DirectedGraph.Node#removeEdge(java.lang.Object)
     */
    @Override
    public boolean removeEdge(T dest) {
      @SuppressWarnings("synthetic-access")
      final HashNode node = graph.get(dest);

      if (node == null) {
        throw new NoSuchElementException("unknown destination node");
      }
      return edges.remove(node);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      return Objects.hashCode(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return edges.stream()
        .map(
          e -> String.valueOf(e.getValue())
        )
        .collect(Collectors.joining("-", value + "=", ""));
    }
  }

  /**
   * Holds the load factor for this graph.
   *
   * @author paouelle
   */
  private final float loadFactor;

  /**
   * Holds the initial capacity for this graph.
   *
   * @author paouelle
   */
  private final int initialCapacity;

  /**
   * Holds the graph.
   *
   * @author paouelle
   */
  private final Map<T, HashNode> graph;

  /**
   * Holds the node set view of this graph.
   *
   * @author paouelle
   */
  private transient Set<Node<T>> nodeSet = null;

  /**
   * Instantiates a new <code>ConcurrentHashDirectedGraph</code> object with
   * the default initial capacity (16) and the default load factor (0.75).
   *
   * @author paouelle
   */
  public ConcurrentHashDirectedGraph() {
    this(16, 0.75f);
  }

  /**
   * Instantiates a new <code>ConcurrentHashDirectedGraph</code> object with the
   * specified initial capacity and the default load factor (0.75).
   *
   * @param  initialCapacity the initial capacity
   * @throws IllegalArgumentException if the initial capacity is negative
   */
  public ConcurrentHashDirectedGraph(int initialCapacity) {
    this(initialCapacity, 0.75f);
  }

  /**
   * Instantiates a new <code>ConcurrentHashDirectedGraph</code> object with the
   * specified initial capacity and load factor.
   *
   * @param  initialCapacity the initial capacity
   * @param  loadFactor he load factor
   * @throws IllegalArgumentException if the initial capacity is negative
   *         or the load factor is non-positive
   */
  public ConcurrentHashDirectedGraph(int initialCapacity, float loadFactor) {
    this.loadFactor = loadFactor;
    this.initialCapacity = initialCapacity;
    this.graph = new ConcurrentHashMap<>(initialCapacity, loadFactor);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#size()
   */
  @Override
  public int size() {
    return graph.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    return graph.isEmpty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#contains(java.lang.Object)
   */
  @Override
  public boolean contains(Object o) {
    return graph.containsKey(o);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    final Iterator<Map.Entry<T, HashNode>> i = graph.entrySet().iterator();

    return new Iterator<T>() {
      Map.Entry<T, HashNode> current = null;

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }
      @Override
      public T next() {
        this.current = i.next();
        return current.getKey();
      }
      @Override
      @SuppressWarnings("synthetic-access")
      public void remove() {
        org.apache.commons.lang3.Validate.validState(current != null);
        i.remove();
        // we need to remove all edges to the removed node
        graph.values().stream()
          .forEach(
            n -> n.edges.remove(current)
          );
        this.current = null;
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Collection#stream()
   */
  @Override
  public Stream<T> stream() {
    return graph.keySet().stream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Collection#parallelStream()
   */
  @Override
  public Stream<T> parallelStream() {
    return graph.keySet().parallelStream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Iterable#forEach(java.util.function.Consumer)
   */
  @Override
  public void forEach(Consumer<? super T> action) {
    graph.keySet().forEach(action);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#toArray()
   */
  @Override
  public Object[] toArray() {
    return graph.keySet().toArray();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#toArray(java.lang.Object[])
   */
  @Override
  public <A> A[] toArray(A[] a) {
    return graph.keySet().toArray(a);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#get(java.lang.Object)
   */
  @Override
  public Node<T> get(T val) {
    return graph.get(val);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#add(java.lang.Object)
   */
  @Override
  public boolean add(T e) {
    return graph.putIfAbsent(e, new HashNode(e)) == null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#remove(java.lang.Object)
   */
  @Override
  @SuppressWarnings("synthetic-access")
  public boolean remove(Object o) {
    final HashNode node = graph.remove(o);

    if (node != null) {
      // we need to remove all edges to the removed node
      graph.values().stream()
        .forEach(
          n -> n.edges.remove(node)
        );
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#containsAll(java.util.Collection)
   */
  @Override
  public boolean containsAll(Collection<?> c) {
    return graph.keySet().containsAll(c);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#addAll(java.util.Collection)
   */
  @Override
  public boolean addAll(Collection<? extends T> c) {
    boolean added = false;

    for (final T e: c) {
      if (add(e)) {
        added = true;
      }
    }
    return added;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#retainAll(java.util.Collection)
   */
  @Override
  public boolean retainAll(Collection<?> c) {
    boolean removed = false;

    for (final Iterator<T> i = iterator(); i.hasNext(); ) {
      if (!c.contains(i.next())) {
        i.remove();
        removed = true;
      }
    }
    return removed;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#removeAll(java.util.Collection)
   */
  @Override
  @SuppressWarnings("synthetic-access")
  public boolean removeAll(Collection<?> c) {
    boolean removed = false;

    for (final Iterator<Map.Entry<T, HashNode>> i = graph.entrySet().iterator(); i.hasNext(); ) {
      final Map.Entry<T, HashNode> e = i.next();

      if (c.contains(e.getKey())) {
        i.remove(); // remove the whole node
        removed = true;
      } else { // remove all edges to elements of c
        for (final Iterator<HashNode> j = e.getValue().edges.iterator(); j.hasNext(); ) {
          if (c.contains(j.next().getValue())) { // remove the edge
            j.remove();
            removed = true;
          }
        }
      }
    }
    return removed;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#clear()
   */
  @Override
  public void clear() {
    graph.clear();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#nodeSet()
   */
  @Override
  @SuppressWarnings("synthetic-access")
  public Set<DirectedGraph.Node<T>> nodeSet() {
    final Set<Node<T>> ns;

    return (ns = nodeSet) == null ? (this.nodeSet = new NodeSet()) : ns;
  }

  @SuppressWarnings("javadoc")
  private final class NodeSet extends AbstractSet<Node<T>> {
    @Override
    @SuppressWarnings("synthetic-access")
    public final int size() {
      return graph.size();
    }
    @Override
    public final void clear() {
      ConcurrentHashDirectedGraph.this.clear();
    }
    @Override
    public final Iterator<Node<T>> iterator() {
      @SuppressWarnings("synthetic-access")
      final Iterator<Map.Entry<T, HashNode>> i = graph.entrySet().iterator();

      return new Iterator<Node<T>>() {
        Map.Entry<T, HashNode> current = null;

        @Override
        public boolean hasNext() {
          return i.hasNext();
        }
        @Override
        public Node<T> next() {
          this.current = i.next();
          return current.getValue();
        }
        @Override
        @SuppressWarnings("synthetic-access")
        public void remove() {
          org.apache.commons.lang3.Validate.validState(current != null);
          i.remove();
          // we need to remove all edges to the removed node
          graph.values().stream()
            .forEach(
              n -> n.edges.remove(current)
            );
          this.current = null;
        }
      };
    }
    @Override
    public final boolean contains(Object o) {
      if (!(o instanceof Node)) {
        return false;
      }
      return contains(((Node<?>)o).getValue());
    }
    @Override
    public final boolean remove(Object o) {
      if (o instanceof Node) {
        return remove(((Node<?>)o).getValue());
      }
      return false;
    }
    @Override
    @SuppressWarnings({"rawtypes", "cast", "unchecked", "synthetic-access"})
    public final Spliterator<Node<T>> spliterator() {
      return (Spliterator<Node<T>>)(Spliterator)graph.values().spliterator();
    }
    @Override
    @SuppressWarnings("synthetic-access")
    public final void forEach(Consumer<? super Node<T>> action) {
      graph.values().forEach(action);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#add(java.lang.Object, java.lang.Object)
   */
  @Override
  public void add(T start, T dest) {
    HashNode sn = new HashNode(start);
    final HashNode old = graph.putIfAbsent(start, sn);

    if (old != null) { // we already had one so continue with it
      sn = old;
    }
    sn.add(dest);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#addEdge(java.lang.Object, java.lang.Object)
   */
  @Override
  public void addEdge(T start, T dest) {
    final HashNode sn = graph.get(start);

    if (sn == null) {
      throw new NoSuchElementException("unknown starting node");
    }
    sn.addEdge(dest);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#removeEdge(java.lang.Object, java.lang.Object)
   */
  @Override
  public boolean removeEdge(T start, T dest) {
    final HashNode sn = graph.get(start);

    if (sn == null) {
      throw new NoSuchElementException("unknown starting node");
    }
    return sn.removeEdge(dest);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#edgeExists(java.lang.Object, java.lang.Object)
   */
  @Override
  public boolean edgeExists(T start, T dest) {
    final HashNode sn = graph.get(start);

    if (sn == null) {
      throw new NoSuchElementException("unknown starting node");
    }
    return sn.edgeExists(dest);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#edgesFrom(java.lang.Object)
   */
  @Override
  public Stream<T> edgesFrom(T node) {
    final HashNode n = graph.get(node);

    if (n == null) {
      throw new NoSuchElementException("unknown node");
    }
    return n.edges()
      .map(e -> e.getValue());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.commons.collections.DirectedGraph#getEdgesFrom(java.lang.Object)
   */
  @Override
  public Set<T> getEdgesFrom(T node) {
    final HashNode n = graph.get(node);

    if (n == null) {
      throw new NoSuchElementException("unknown node");
    }
    return Collections.unmodifiableSet(
      n.getEdges().stream()
        .map(e -> e.getValue())
        .collect(Collectors.toSet())
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return super.hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  /**
   * Returns a shallow copy of this <tt>ConcurrentHashDirectedGraph</tt>
   * instance: the values themselves are not cloned.
   *
   * @return a shallow copy of this graph
   */
  @Override
  public Object clone() {
    final ConcurrentHashDirectedGraph<T> clone
      = new ConcurrentHashDirectedGraph<>(initialCapacity, loadFactor);

    clone.addAll(graph.keySet());
    clone.nodeSet().forEach(
      n -> graph.get(n.getValue()).edges()
        .forEach(
          e -> n.addEdge(e.getValue())
        )
    );
    return clone;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return graph.values()
      .stream()
      .map(
        n -> n.edges()
          .map(
            e -> (e.getValue() == this) ? "(this graph)" : String.valueOf(e.getValue())
          )
          .collect(Collectors.joining(
            "-", (n.getValue() == this) ? "(this graph)" : String.valueOf(n.getValue()) + '=', "")
          )
      )
      .collect(Collectors.joining(", ", "{", "}"));
  }
}
