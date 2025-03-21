/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.client;

import com.alibaba.graphscope.common.client.channel.ChannelFetcher;
import com.alibaba.graphscope.common.client.type.ExecutionRequest;
import com.alibaba.graphscope.common.client.type.ExecutionResponseListener;
import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.QueryTimeoutConfig;
import com.alibaba.graphscope.common.metric.MetricsTool;
import com.alibaba.graphscope.gremlin.plugin.QueryLogger;

/**
 * client to submit request to remote engine service
 *
 * @param <C>
 */
public abstract class ExecutionClient<C> {
    protected final ChannelFetcher<C> channelFetcher;

    public ExecutionClient(ChannelFetcher<C> channelFetcher) {
        this.channelFetcher = channelFetcher;
    }

    public abstract void submit(
            ExecutionRequest request,
            ExecutionResponseListener listener,
            QueryTimeoutConfig timeoutConfig,
            QueryLogger queryLogger)
            throws Exception;

    public abstract void close() throws Exception;

    public static class Factory {
        public static ExecutionClient create(
                Configs configs, ChannelFetcher channelFetcher, MetricsTool metricsTool) {
            switch (channelFetcher.getType()) {
                case RPC:
                    return new RpcExecutionClient(configs, channelFetcher, metricsTool);
                case HTTP:
                    return new HttpExecutionClient(configs, channelFetcher);
                default:
                    throw new IllegalArgumentException("unknown channel fetcher type");
            }
        }
    }
}
