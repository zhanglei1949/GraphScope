/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class FlatJoinToCommonRule extends FlatJoinRule {
    private ExpandFlatter expandFlatter;
    private final GraphBuilder builder;

    public FlatJoinToCommonRule(GraphBuilder builder) {
        this.builder =
                GraphBuilder.create(
                        builder.getContext(),
                        (GraphOptCluster) builder.getCluster(),
                        builder.getRelOptSchema());
    }

    @Override
    protected boolean matches(LogicalJoin join) {
        List<RexGraphVariable> vars = Lists.newArrayList();
        List<RexNode> others = Lists.newArrayList();
        classifyJoinCondition(join.getCondition(), vars, others);
        if (vars.size() != 2 || !others.isEmpty()) {
            return false;
        }
        RexGraphVariable joinKey0 = vars.get(0);
        RexGraphVariable joinKey1 = vars.get(1);
        List<GraphLogicalSingleMatch> matches = Lists.newArrayList();
        getMatchBeforeJoin(join.getRight(), matches);
        if (matches.size() != 1) return false;
        RelNode sentence = matches.get(0).getSentence();
        int sentenceStartId = getSource(sentence).getAliasId();
        int sentenceEndId = getAliasId(sentence);
        if (sentenceStartId != joinKey0.getAliasId() || sentenceEndId != joinKey1.getAliasId()) {
            return false;
        }
        CommonTableScan common =
                new CommonTableScan(
                        join.getCluster(), join.getTraitSet(), new CommonOptTable(join.getLeft()));
        // distinct by joinKey0
        RelNode expandSource = builder.push(common).aggregate(builder.groupKey(joinKey0)).build();
        expandFlatter =
                new ExpandFlatter(
                        expandSource,
                        new AliasNameWithId(
                                joinKey0.getName().split("\\.")[0], joinKey0.getAliasId()),
                        false,
                        others);
        return true;
    }

    @Override
    protected RelNode perform(LogicalJoin join) {
        return join.copy(
                join.getTraitSet(),
                ImmutableList.of(
                        new CommonTableScan(
                                join.getCluster(),
                                join.getTraitSet(),
                                new CommonOptTable(join.getLeft())),
                        join.getRight().accept(expandFlatter)));
    }
}
