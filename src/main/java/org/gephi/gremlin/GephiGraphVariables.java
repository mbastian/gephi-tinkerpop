/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.gephi.gremlin;

import java.util.Optional;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.gephi.graph.api.Graph;

public final class GephiGraphVariables implements org.apache.tinkerpop.gremlin.structure.Graph.Variables {

    private final Graph graph;

    GephiGraphVariables(Graph graph) {
        this.graph = graph;
    }

    @Override
    public Set<String> keys() {
        return graph.getAttributeKeys();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) graph.getAttribute(key));
    }

    @Override
    public void remove(final String key) {
        graph.setAttribute(key, key);
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        try {
            graph.setAttribute(key, value);
        } catch (final IllegalArgumentException e) {
            throw org.apache.tinkerpop.gremlin.structure.Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
