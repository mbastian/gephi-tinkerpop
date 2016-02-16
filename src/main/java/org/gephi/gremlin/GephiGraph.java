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

import java.io.File;
import java.util.ArrayList;
import org.gephi.graph.api.*;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

@org.apache.tinkerpop.gremlin.structure.Graph.OptIn("org.apache.tinkerpop.gremlin.structure.StructureStandardSuite")
public class GephiGraph implements org.apache.tinkerpop.gremlin.structure.Graph {

    static final org.apache.commons.configuration.Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {
        {
            this.setProperty(org.apache.tinkerpop.gremlin.structure.Graph.GRAPH, GephiGraph.class.getName());
        }
    };

    ///////
    public static final String GREMLIN_GEPHIGRAPH_GRAPH_LOCATION = "gremlin.gephigraph.graphLocation";
    public static final String GREMLIN_GEPHIGRAPH_GRAPH_FORMAT = "gremlin.gephigraph.graphFormat";
    //
    private final GephiFeatures features = new GephiFeatures();
    private final GephiGraphVariables variables;
    private final org.apache.commons.configuration.Configuration configuration;
    private final String graphLocation;
    private final String graphFormat;

    private GraphModel graphModel;
    private Graph graph;

    public GephiGraph(final org.apache.commons.configuration.Configuration configuration) {
        this(configuration, GraphModel.Factory.newInstance(getDefaultConfiguration()));
    }

    public GephiGraph(final org.apache.commons.configuration.Configuration configuration, GraphModel graphModel) {
        this.graphModel = graphModel;
        this.graph = graphModel.getGraph();
        this.variables = new GephiGraphVariables(graph);
        this.configuration = configuration;

        graphLocation = configuration.getString(GREMLIN_GEPHIGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_GEPHIGRAPH_GRAPH_FORMAT, null);

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null)) {
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_GEPHIGRAPH_GRAPH_LOCATION, GREMLIN_GEPHIGRAPH_GRAPH_FORMAT));
        }

        if (graphLocation != null) {
            loadGraph();
        }
    }

    private static Configuration getDefaultConfiguration() {
        Configuration config = new Configuration();
        config.setEdgeWeightColumn(Boolean.FALSE);
        return config;
    }

    public static GephiGraph open() {
        return open(EMPTY_CONFIGURATION);
    }

    public static GephiGraph open(final org.apache.commons.configuration.Configuration configuration) {
        return new GephiGraph(configuration);
    }

    public Features getFeatures() {
        return features;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (idValue instanceof UUID) {
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        } else if (idValue != null && !(idValue instanceof String)) {
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }

        if (idValue != null && graphModel.getGraph().getNode(idValue) != null) {
            throw Exceptions.vertexWithIdAlreadyExists(idValue);
        }

        Node node;
        if (idValue == null) {
            node = graphModel.factory().newNode();
        } else {
            node = graphModel.factory().newNode(idValue);
        }
        node.setLabel(label);
        graph.addNode(node);

        Vertex vertex = new GephiVertex(node, this);
        ElementHelper.attachProperties(vertex, keyValues);

        return new GephiVertex(node, this);
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public org.apache.commons.configuration.Configuration configuration() {
        return configuration;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        if (vertexIds.length == 0) {
            return IteratorUtils.list(IteratorUtils.map(graph.getNodes().iterator(), node -> (Vertex) new GephiVertex(node, this))).iterator();
        }
        Object[] ids = vertexIds;
        if (Vertex.class.isAssignableFrom(vertexIds[0].getClass())) {
            if (!Stream.of(vertexIds).allMatch(id -> Vertex.class.isAssignableFrom(id.getClass()))) {
                throw org.apache.tinkerpop.gremlin.structure.Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
            ids = Stream.of(vertexIds).map(id -> ((Vertex) id).id()).toArray();
        } else {
            final Class<?> firstClass = ids[0].getClass();
            if (!Stream.of(ids).map(Object::getClass).allMatch(firstClass::equals)) {
                throw org.apache.tinkerpop.gremlin.structure.Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
        }

        List<Vertex> res = new ArrayList<>();
        for (Object id : ids) {
            Node node = graph.getNode(id);
            if (node != null) {
                res.add(new GephiVertex(node, this));
            }
        }
        return res.iterator();
    }

    @Override
    public Iterator<org.apache.tinkerpop.gremlin.structure.Edge> edges(Object... edgeIds) {
        if (edgeIds.length == 0) {
            return IteratorUtils.list(IteratorUtils.map(graph.getEdges().iterator(), edge -> (org.apache.tinkerpop.gremlin.structure.Edge) new GephiEdge(edge, this))).iterator();
        }
        Object[] ids = edgeIds;
        if (org.apache.tinkerpop.gremlin.structure.Edge.class.isAssignableFrom(edgeIds[0].getClass())) {
            if (!Stream.of(edgeIds).allMatch(id -> org.apache.tinkerpop.gremlin.structure.Edge.class.isAssignableFrom(id.getClass()))) {
                throw org.apache.tinkerpop.gremlin.structure.Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
            ids = Stream.of(edgeIds).map(id -> ((org.apache.tinkerpop.gremlin.structure.Edge) id).id()).toArray();
        } else {
            final Class<?> firstClass = ids[0].getClass();
            if (!Stream.of(ids).map(Object::getClass).allMatch(firstClass::equals)) {
                throw org.apache.tinkerpop.gremlin.structure.Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
        }

        List<org.apache.tinkerpop.gremlin.structure.Edge> res = new ArrayList<>();
        for (Object id : ids) {
            Edge edge = graph.getEdge(id);
            if (edge != null) {
                res.add(new GephiEdge(edge, this));
            }
        }
        return res.iterator();
    }

    @Override
    public <C extends GraphComputer> C compute(
            final Class<C> graphComputerClass
    ) {
        throw org.apache.tinkerpop.gremlin.structure.Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw org.apache.tinkerpop.gremlin.structure.Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public <I extends Io> I io(
            final Io.Builder<I> builder
    ) {
        return (I) builder.graph(this).registry(GephiIoRegistry.getInstance()).create();
    }

    @Override
    public Variables variables() {
        return variables;
    }

    public Graph getGraph() {
        return this.graphModel.getGraph();
    }

    public GraphModel getGraphModel() {
        return this.graphModel;
    }

    @Override
    public void close() {
        if (graphLocation != null) {
            saveGraph();
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, this.graphModel.getGraph().toString());
    }

    private void loadGraph() {
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(IoCore.graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(IoCore.graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    //TODO
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    private void saveGraph() {
        final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(IoCore.graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(IoCore.graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                //TODO
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }

    @Override
    public Features features() {
        return features;
    }

    public class GephiFeatures implements Features {

        private final GephiGraphFeatures graphFeatures = new GephiGraphFeatures();
        private final GephiEdgeFeatures edgeFeatures = new GephiEdgeFeatures();
        private final GephiVertexFeatures vertexFeatures = new GephiVertexFeatures();

        private GephiFeatures() {
        }

        @Override
        public Features.GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public Features.EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public Features.VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public class GephiElementFeatures implements Features.ElementFeatures {

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean willAllowId(Object id) {
            return id instanceof String;
        }
    }

    public class GephiDataTypeFeatures implements Features.DataTypeFeatures {

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }
    }

    public class GephiGraphFeatures implements Features.GraphFeatures {

        private final GephiVariableFeatures variablesFeatures = new GephiVariableFeatures();

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public Features.VariableFeatures variables() {
            return variablesFeatures;
        }
    }

    public class GephiVariableFeatures extends GephiDataTypeFeatures implements Features.VariableFeatures {

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }
    }

    public class GephiEdgeFeatures extends GephiElementFeatures implements Features.EdgeFeatures {

        private final GephiPropertyFeatures edgePropertyFeatures = new GephiPropertyFeatures();

        private GephiEdgeFeatures() {
        }

        @Override
        public Features.EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }
    }

    public class GephiVertexFeatures extends GephiElementFeatures implements Features.VertexFeatures {

        private final GephiPropertyFeatures vertexPropertyFeatures = new GephiPropertyFeatures();

        private GephiVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMultiProperties() {
            return false;
        }
    }

    public class GephiPropertyFeatures extends GephiDataTypeFeatures implements Features.VertexPropertyFeatures, Features.EdgePropertyFeatures {

        private GephiPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }
    }
}
