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

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class FlatJoinToExpandRule extends FlatJoinRule {
    private ExpandFlatter expandFlatter;

    @Override
    protected boolean matches(LogicalJoin join) {
        RexNode condition = join.getCondition();
        List<RexNode> others = Lists.newArrayList();
        RexGraphVariable var = joinByOneColumn(condition, others);
        if (var == null) return false;
        List<GraphLogicalSingleMatch> matches = Lists.newArrayList();
        getMatchBeforeJoin(join.getRight(), matches);
        if (matches.size() != 1) return false;
        RelNode sentence = matches.get(0).getSentence();
        if (hasPxdWithUntil(sentence)) return false;
        if (hasNodeEqualFilter(sentence)) return false;
        GraphLogicalSource source = getSource(sentence);
        List<Integer> startEndAliasIds =
                Lists.newArrayList(getAliasId(source), getAliasId(sentence));
        // check whether the sentence starts or ends with the alias id in the join condition
        boolean contains = startEndAliasIds.contains(var.getAliasId());
        if (contains) {
            this.expandFlatter =
                    new ExpandFlatter(
                            join.getLeft(),
                            new AliasNameWithId(var.getName().split("\\.")[0], var.getAliasId()),
                            join.getJoinType() == JoinRelType.LEFT,
                            others);
        }
        return contains;
    }

    @Override
    protected RelNode perform(LogicalJoin join) {
        return join.getRight().accept(this.expandFlatter);
    }
}
