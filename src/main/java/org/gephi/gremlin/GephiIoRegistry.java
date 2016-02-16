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

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import java.io.IOException;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.impl.utils.DataInputOutput;
import static org.gephi.gremlin.GephiGraph.EMPTY_CONFIGURATION;

/**
 * An implementation of the {@link IoRegistry} interface that provides
 * serializers with custom configurations for implementation specific classes
 * that might need to be serialized. This registry allows a {@link TinkerGraph}
 * to be serialized directly which is useful for moving small graphs around on
 * the network.
 * <p/>
 * Most vendors need not implement this kind of custom serializer as they will
 * deal with much larger graphs that wouldn't be practical to serialize in this
 * fashion. This is a bit of a special case for TinkerGraph given its in-memory
 * status. Typical implementations would create serializers for a complex vertex
 * identifier or a custom data class like a "geographic point".
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GephiIoRegistry extends AbstractIoRegistry {

    private static final GephiIoRegistry INSTANCE = new GephiIoRegistry();

    private GephiIoRegistry() {
        register(GryoIo.class, GephiGraph.class, new GephiGraphGryoSerializer());
    }

    public static GephiIoRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Provides a method to serialize an entire {@link TinkerGraph} into itself
     * for Gryo. This is useful when shipping small graphs around through
     * Gremlin Server. Reuses the existing Kryo instance for serialization.
     */
    final static class GephiGraphGryoSerializer extends Serializer<GephiGraph> {

        @Override
        public void write(final Kryo kryo, final Output output, final GephiGraph graph) {
            DataInputOutput dos = new DataInputOutput();
            try {

                GraphModel.Serialization.write(dos, graph.getGraphModel());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {

            }

            final byte[] bytes = dos.toByteArray();
            output.writeInt(bytes.length);
            output.write(bytes);
        }

        @Override
        public GephiGraph read(final Kryo kryo, final Input input, final Class<GephiGraph> tinkerGraphClass) {
            final int len = input.readInt();
            final byte[] bytes = input.readBytes(len);

            GraphModel graphModel;
            DataInputOutput dos = new DataInputOutput(bytes);
            try {

                graphModel = GraphModel.Serialization.read(dos);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {

            }

            return new GephiGraph(EMPTY_CONFIGURATION, graphModel);
        }
    }
}
