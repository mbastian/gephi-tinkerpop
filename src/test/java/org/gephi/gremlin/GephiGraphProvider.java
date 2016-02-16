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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.GraphTest;

public class GephiGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {
        {
            add(GephiEdge.class);
            add(GephiElement.class);
            add(GephiGraph.class);
            add(GephiGraphVariables.class);
            add(GephiColumnProperty.class);
            add(GephiVertex.class);
            add(GephiVertexProperty.class);
        }
    };

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {
            {
                put(Graph.GRAPH, GephiGraph.class.getName());
                if (requiresPersistence(test, testMethodName)) {
                    put(GephiGraph.GREMLIN_GEPHIGRAPH_GRAPH_FORMAT, "gryo");
                    final File tempDir = TestHelper.makeTestDataPath(test, "temp");
                    put(GephiGraph.GREMLIN_GEPHIGRAPH_GRAPH_LOCATION,
                            tempDir.getAbsolutePath() + File.separator + testMethodName + ".kryo");
                }
            }
        };
    }

    protected static boolean requiresPersistence(final Class<?> test, final String testMethodName) {
        return test == GraphTest.class && testMethodName.equals("shouldPersistDataOnClose");
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) {
            graph.close();
        }

        // in the even the graph is persisted we need to clean up
        final String graphLocation = configuration.getString(GephiGraph.GREMLIN_GEPHIGRAPH_GRAPH_LOCATION, null);
        if (graphLocation != null) {
            final File f = new File(graphLocation);
            f.delete();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

}
