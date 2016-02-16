/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.gephi.gremlin;

import org.gephi.graph.api.Node;
import org.gephi.graph.api.Edge;

import java.util.*;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.gephi.graph.api.Column;
import org.gephi.graph.api.DirectedGraph;
import org.gephi.graph.api.Origin;
import org.gephi.graph.api.Table;

public class GephiVertex extends GephiElement<Node> implements Vertex {

    public GephiVertex(Node node, GephiGraph graph) {
        super(graph, node);
    }

    @Override
    protected Table getTable() {
        return graph.getGraphModel().getNodeTable();
    }

    @Override
    protected boolean isValid() {
        return element.getStoreId() != -1;
    }

    @Override
    public org.apache.tinkerpop.gremlin.structure.Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (vertex == null) {
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        }

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        if (idValue instanceof UUID) {
            throw org.apache.tinkerpop.gremlin.structure.Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        } else if (idValue != null && !(idValue instanceof String)) {
            throw org.apache.tinkerpop.gremlin.structure.Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }

        if (idValue != null && graph.getGraph().getEdge(idValue) != null) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        }

        Node target = ((GephiVertex) vertex).element;
        int type = graph.getGraphModel().addEdgeType(label);

        Edge edge;
        if (idValue == null) {
            edge = graph.getGraphModel().factory().newEdge(element, target, type, true);
        } else {
            edge = graph.getGraphModel().factory().newEdge(idValue, element, target, type, 1.0, true);
        }
        edge.setLabel(label);

        GephiEdge gephiEdge = new GephiEdge(edge, graph);
        if (graph.getGraph().addEdge(edge)) {
            ElementHelper.attachProperties(gephiEdge, keyValues);
        }

        return gephiEdge;
    }

    @Override
    public Iterator<org.apache.tinkerpop.gremlin.structure.Edge> edges(Direction direction, String... labels) {
        List<Iterator<org.apache.tinkerpop.gremlin.structure.Edge>> itrs = new ArrayList<>();

        DirectedGraph directedGraph = (DirectedGraph) graph.getGraph();
        if (labels.length > 0) {
            for (String label : labels) {
                int type = graph.getGraphModel().getEdgeType(label);
                if (type != -1) {
                    switch (direction) {
                        case IN:
                            itrs.add(IteratorUtils.map(directedGraph.getInEdges(element, type).iterator(), edge -> new GephiEdge(edge, graph)));
                            break;
                        case OUT:
                            itrs.add(IteratorUtils.map(directedGraph.getOutEdges(element, type).iterator(), edge -> new GephiEdge(edge, graph)));
                            break;
                        case BOTH:
                            itrs.add(IteratorUtils.map(directedGraph.getEdges(element, type).iterator(), edge -> new GephiEdge(edge, graph)));
                            break;
                    }

                }
            }
        } else {
            switch (direction) {
                case IN:
                    itrs.add(IteratorUtils.map(directedGraph.getInEdges(element).iterator(), edge -> new GephiEdge(edge, graph)));
                    break;
                case OUT:
                    itrs.add(IteratorUtils.map(directedGraph.getOutEdges(element).iterator(), edge -> new GephiEdge(edge, graph)));
                    break;
                case BOTH:
                    itrs.add(IteratorUtils.map(directedGraph.getEdges(element).iterator(), edge -> new GephiEdge(edge, graph)));
                    break;
            }
        }
        return IteratorUtils.list(IteratorUtils.concat(itrs.toArray(new Iterator[0]))).iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... labels) {
        return IteratorUtils.map(edges(direction, labels), edge -> new GephiVertex(graph.getGraph().getOpposite(element, ((GephiEdge) edge).element), graph));
    }

    @Override
    public void remove() {
        graph.getGraph().removeNode(element);
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        Table table = getTable();
        Column column = table.getColumn(key);
        if (column == null) {
            return VertexProperty.empty();
        } else {
            Object r = element.getAttribute(column);
            if (r != null) {
                return new GephiVertexProperty(this, column);

            } else {
                return VertexProperty.empty();
            }
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);

        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        if (optionalId.isPresent()) {
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }

        Table table = getTable();
        Column col = table.getColumn(key);
        if (col == null) {
            col = table.addColumn(key, null, value.getClass(), Origin.DATA, null, true);
            table.addColumn(key + GephiVertexProperty.PROPERTY_SUFFIX, null, Map.class, Origin.PROPERTY, null, false);
        }
        element.setAttribute(col, value);

        GephiVertexProperty vertexProperty = new GephiVertexProperty(this, col);
        ElementHelper.attachProperties(vertexProperty, keyValues);

        return vertexProperty;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Table table = getTable();
        if (propertyKeys.length == 1) {
            Column column = table.getColumn(propertyKeys[0]);
            if (column != null && !column.isProperty() && element.getAttribute(column) != null) {
                final VertexProperty<V> property = new GephiVertexProperty(this, column);
                return IteratorUtils.of(property);
            } else {
                return Collections.emptyIterator();
            }
        } else {
            List<VertexProperty<V>> props = new ArrayList<>();
            if (propertyKeys.length > 0) {
                for (String s : propertyKeys) {
                    Column column = table.getColumn(s);
                    if (column != null && !column.isProperty() && element.getAttribute(column) != null) {
                        props.add(new GephiVertexProperty<>(this, column));
                    }
                }
            } else {
                for (Column col : table) {
                    if (!col.isProperty() && element.getAttribute(col) != null) {
                        props.add(new GephiVertexProperty<>(this, col));
                    }
                }
            }
            return props.iterator();
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    protected boolean isValidColumn(Column col) {
        return !col.isProperty();
    }
}
