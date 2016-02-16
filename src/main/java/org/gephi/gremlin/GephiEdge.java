package org.gephi.gremlin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.gephi.graph.api.Column;
import org.gephi.graph.api.Edge;
import org.gephi.graph.api.Origin;
import org.gephi.graph.api.Table;
import org.gephi.graph.impl.GraphStoreConfiguration;

public class GephiEdge extends GephiElement<Edge> implements org.apache.tinkerpop.gremlin.structure.Edge {

    public GephiEdge(Edge edge, GephiGraph graph) {
        super(graph, edge);
    }

    @Override
    protected Table getTable() {
        return graph.getGraphModel().getEdgeTable();
    }

    @Override
    protected boolean isValid() {
        return element.getStoreId() != -1;
    }

    @Override
    public Vertex outVertex() {
        return new GephiVertex(element.getSource(), graph);
    }

    @Override
    public Vertex inVertex() {
        return new GephiVertex(element.getTarget(), graph);
    }

    @Override
    public void remove() {
        graph.getGraph().removeEdge(element);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(outVertex());
            case IN:
                return IteratorUtils.of(inVertex());
            default:
                return IteratorUtils.of(outVertex(), inVertex());
        }
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Table table = getTable();
        if (propertyKeys.length == 1) {
            Column column = table.getColumn(propertyKeys[0]);
            if (isValidColumn(column) && element.getAttribute(column) != null) {
                final Property<V> property = new GephiColumnProperty<>(this, column);
                return IteratorUtils.of(property);
            } else {
                return Collections.emptyIterator();
            }
        } else {
            List<Property<V>> props = new ArrayList<>();
            if (propertyKeys.length > 0) {
                for (String s : propertyKeys) {
                    Column column = table.getColumn(s);
                    if (isValidColumn(column) && element.getAttribute(column) != null) {
                        props.add(new GephiColumnProperty<>(this, column));
                    }
                }
            } else {
                for (Column col : table) {
                    if (isValidColumn(col) && element.getAttribute(col) != null) {
                        props.add(new GephiColumnProperty<>(this, col));
                    }
                }
            }
            return props.iterator();
        }
    }

    @Override
    protected boolean isValidColumn(Column col) {
        return col != null && (!col.isProperty() || col.getIndex() == GraphStoreConfiguration.EDGE_WEIGHT_INDEX);
    }

    @Override
    public <V> Property<V> property(String key) {
        Table table = getTable();
        Column column = table.getColumn(key);
        if (column == null) {
            return Property.empty();
        } else {
            return new GephiColumnProperty<>(this, column);
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        if (!isValid()) {
            throw new IllegalStateException(String.format("%s with id %s was removed.", getClass().getSimpleName(), element.getId()));
        }

        ElementHelper.validateProperty(key, value);
        Table table = getTable();
        Column col = table.getColumn(key);
        if (col == null) {
            col = table.addColumn(key, null, value.getClass(), Origin.DATA, null, true);
        }
        element.setAttribute(col, value);

        return new GephiColumnProperty<>(this, col);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
