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
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.gephi.graph.api.Column;
import org.gephi.graph.api.Element;
import org.gephi.graph.api.Table;

public abstract class GephiElement<K extends Element> implements org.apache.tinkerpop.gremlin.structure.Element {

    protected final GephiGraph graph;
    protected K element;

    public GephiElement(GephiGraph graph, K element) {
        this.graph = graph;
        this.element = element;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object obj) {
        return ElementHelper.areEqual(this, obj);
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public Set<String> keys() {
        Set<String> keys = new HashSet<>();
        for (Column col : getTable()) {
            if (isValidColumn(col) && element.getAttribute(col) != null) {
                keys.add(col.getId());
            }
        }
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public <V> V value(String key) throws NoSuchElementException {
        Column col = getTable().getColumn(key);
        if (col == null) {
            throw Property.Exceptions.propertyDoesNotExist(this, key);
        }
        V val = (V) element.getAttribute(col);
        if (val == null) {
            throw Property.Exceptions.propertyDoesNotExist(this, key);
        }
        return val;
    }

    @Override
    public Object id() {
        return element.getId();
    }

    @Override
    public String label() {
        return element.getLabel();
    }

    protected abstract boolean isValidColumn(Column col);

    protected abstract Table getTable();

    protected abstract boolean isValid();
}
