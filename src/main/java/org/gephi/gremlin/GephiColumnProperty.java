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

import java.util.NoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.gephi.graph.api.Column;

public final class GephiColumnProperty<V> implements Property<V> {

    protected final GephiElement element;
    protected final Column column;

    public GephiColumnProperty(final GephiElement element, final Column column) {
        this.element = element;
        this.column = column;
    }

    @Override
    public String key() {
        return column.getId();
    }

    @Override
    public V value() throws NoSuchElementException {
        return (V) element.element.getAttribute(column);
    }

    @Override
    public boolean isPresent() {
        return element.element.getAttribute(column) != null;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {
        element.element.removeAttribute(column);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
