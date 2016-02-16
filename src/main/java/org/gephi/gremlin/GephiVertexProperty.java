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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.gephi.graph.api.Column;

public class GephiVertexProperty<V> implements VertexProperty<V> {

    protected static final String PROPERTY_SUFFIX = "_properties";
    protected final GephiVertex vertex;
    protected final Column column;
    protected final Column propertyCol;

    public GephiVertexProperty(final GephiVertex vertex, final Column column) {
        this.vertex = vertex;
        this.column = column;
        this.propertyCol = column.getTable().getColumn(column.getId() + PROPERTY_SUFFIX);
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public Object id() {
        return (long) (column.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public String key() {
        return column.getId();
    }

    @Override
    public V value() throws NoSuchElementException {
        return (V) vertex.element.getAttribute(column);
    }

    @Override
    public boolean isPresent() {
        return vertex.element.getAttribute(column) != null;
    }

    @Override
    public void remove() {
        vertex.element.removeAttribute(column);
        vertex.element.removeAttribute(propertyCol);
    }

    @Override
    public Set<String> keys() {
        Map properties = (Map) vertex.element.getAttribute(propertyCol);
        return null == properties ? Collections.emptySet() : properties.keySet();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... strings) {
        return Collections.EMPTY_LIST.iterator();
    }

    @Override
    public Graph graph() {
        return vertex.graph;
    }

    @Override
    public <U> Property<U> property(final String key) {
        Map properties = (Map) vertex.element.getAttribute(propertyCol);
        if (properties == null) {
            return Property.empty();
        }
        Object val = properties.get(key);
        if (val != null) {
            return new GephiValueProperty(vertex, propertyCol, key);
        }
        return Property.empty();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Map properties = (Map) vertex.element.getAttribute(propertyCol);
        if (properties == null) {
            properties = new HashMap();
        }
        properties.put(key, value);
        vertex.element.setAttribute(propertyCol, properties);
        return new GephiValueProperty(vertex, propertyCol, key);
    }
}
