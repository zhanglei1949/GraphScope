/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.tests.gremlin;

import com.alibaba.graphscope.groot.common.config.StoreConfig;
import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;

public class GrootGraphProvider extends AbstractGraphProvider implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GrootGraphProvider.class);

    private GrootGraph graph;
    private String storeDataPath;

    private Set<LoadGraphWith.GraphData> loadedGraphs = new HashSet<>();

    @Override
    public Map<String, Object> getBaseConfiguration(
            String graphName,
            Class<?> test,
            String testMethodName,
            LoadGraphWith.GraphData loadGraphWith) {
        Properties properties = new Properties();
        try (InputStream ins =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResourceAsStream("gremlin-test.config")) {
            properties.load(ins);
        } catch (IOException e) {
            throw new InvalidArgumentException(e);
        }
        properties.put(Graph.GRAPH, GrootGraph.class.getName());
        if (this.storeDataPath == null) {
            this.storeDataPath = makeTestDirectory(graphName, test, testMethodName);
        }
        properties.put(StoreConfig.STORE_DATA_PATH.getKey(), this.storeDataPath);
        return (Map) properties;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) {
            this.graph = (GrootGraph) graph;
        }
    }

    @Override
    public void loadGraphData(
            Graph graph, LoadGraphWith loadGraphWith, Class testClass, String testName) {
        // other graph data excluding modern is unsupported for ir on groot
        LoadGraphWith.GraphData graphData = LoadGraphWith.GraphData.MODERN;
        if (loadedGraphs.contains(graphData)) return;
        try {
            ((GrootGraph) graph).loadSchema(graphData);
            ((GrootGraph) graph).loadData(graphData);
            loadedGraphs.add(graphData);
        } catch (URISyntaxException | IOException e) {
            logger.error("load schema failed", e);
            throw new InvalidArgumentException(e);
        } catch (InterruptedException e) {
            logger.error("load data failed", e);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return null;
    }

    @Override
    public void close() throws Exception {
        if (this.graph != null) {
            this.graph.getMaxNode().close();
        }
        if (this.storeDataPath != null) {
            deleteDirectory(new File(this.storeDataPath));
        }
    }
}
