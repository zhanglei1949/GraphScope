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
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.Utils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlatJoinToIntersectRule extends FlatJoinRule {
    private final GraphBuilder builder;
    private final Set<String> uniqueNameList;

    private boolean optional;
    private int expandCount;
    private RelNode left;
    private AliasNameWithId leftAlias1;
    private AliasNameWithId leftAlias2;

    public FlatJoinToIntersectRule(GraphBuilder parent) {
        this.builder =
                GraphBuilder.create(
                        parent.getContext(),
                        (GraphOptCluster) parent.getCluster(),
                        parent.getRelOptSchema());
        this.uniqueNameList = Sets.newHashSet();
    }

    @Override
    protected boolean matches(LogicalJoin join) {
        // join by two column
        // todo: add other join conditions
        List<RexGraphVariable> vars = joinByTwoColumns(join.getCondition(), Lists.newArrayList());
        if (vars.size() != 2) return false;
        List<GraphLogicalSingleMatch> matches = Lists.newArrayList();
        getMatchBeforeJoin(join.getRight(), matches);
        if (matches.size() != 1) return false;
        RelNode sentence = matches.get(0).getSentence();
        // currently, only when there are 2 or 3 expand operators in the sentence, we can do the
        // transformation
        expandCount = getExpandCount(sentence);
        if (hasNodeFilter(sentence)
                || hasPxdWithUntil(sentence)
                || !isValidExpandCount(expandCount)) return false;
        GraphLogicalSource source = getSource(sentence);
        List<Integer> startEndAliasIds =
                Lists.newArrayList(getAliasId(source), getAliasId(sentence));
        // check whether the alias ids in the join condition are the start and end of the sentence
        boolean containsAll =
                startEndAliasIds.containsAll(
                        vars.stream().map(k -> k.getAliasId()).collect(Collectors.toList()));
        if (containsAll) {
            optional = join.getJoinType() == JoinRelType.LEFT;
            left = join.getLeft();
            leftAlias1 = new AliasNameWithId(getAliasName(source), startEndAliasIds.get(0));
            leftAlias2 = new AliasNameWithId(getAliasName(sentence), startEndAliasIds.get(1));
        }
        return containsAll;
    }

    @Override
    protected RelNode perform(LogicalJoin join) {
        return join.getRight().accept(new IntersectFlatter());
    }

    // perform the transformation from the join plan to the intersect plan
    // i.e. join(getV1->expand1->source1, getV2->expand2_1->expand2_2->source2) =>
    // intersect(expand2_1, expand2_2)->common(getV1->expand1->source1)
    private class IntersectFlatter extends GraphShuttle {
        public IntersectFlatter() {
            Preconditions.checkArgument(
                    isValidExpandCount(expandCount),
                    "expand count in intersect flatter is invalid");
        }

        @Override
        public RelNode visit(GraphLogicalSingleMatch match) {
            RelNode sentence = match.getSentence();
            if (optional) {
                sentence = sentence.accept(new SetOptional());
            }
            return toIntersect(sentence);
        }

        private MultiJoin toIntersect(RelNode top) {
            List<RelNode> splits = splitByMiddle(top);
            String splitAlias = getAliasName(splits.get(0));
            RelNode leftInput =
                    splits.get(0)
                            .accept(
                                    new ReplaceInput(
                                            leftAlias1,
                                            new CommonTableScan(
                                                    top.getCluster(),
                                                    top.getTraitSet(),
                                                    new CommonOptTable(left))));
            RelNode rightInput =
                    splits.get(1)
                            .accept(
                                    new ReplaceInput(
                                            leftAlias2,
                                            new CommonTableScan(
                                                    top.getCluster(),
                                                    top.getTraitSet(),
                                                    new CommonOptTable(left))));
            RexNode condition =
                    builder.call(
                            GraphStdOperatorTable.EQUALS,
                            builder.push(leftInput).variable(splitAlias),
                            builder.push(rightInput).variable(splitAlias));
            List<RelNode> inputs = Lists.newArrayList(leftInput, rightInput);
            return new MultiJoin(
                    top.getCluster(),
                    inputs,
                    condition,
                    Utils.getOutputType(inputs.get(0)),
                    false,
                    Stream.generate(() -> (RexNode) null)
                            .limit(inputs.size())
                            .collect(Collectors.toList()),
                    Stream.generate(() -> JoinRelType.INNER)
                            .limit(inputs.size())
                            .collect(Collectors.toList()),
                    Stream.generate(() -> (ImmutableBitSet) null)
                            .limit(inputs.size())
                            .collect(Collectors.toList()),
                    ImmutableMap.of(),
                    null);
        }

        private List<RelNode> splitByMiddle(RelNode top) {
            // supposing expand count in the sentence is exact 2 or 3
            RelNode middleParent = top.getInput(0);
            RelNode middle = middleParent.getInput(0);
            String middleAlias = getAliasName(middle);
            // the middle alias is used for intersection which cannot not be 'DEFAULT'
            if (middleAlias == AliasInference.DEFAULT_NAME) {
                middleAlias = generateNewAlias();
                middle = setAlias(middle, middleAlias);
            }
            middleParent.replaceInput(0, toSource(middle));
            return ImmutableList.of(middle, top.accept(new Reverse(top)));
        }

        // should be unique among the operators in the same query
        private String generateNewAlias() {
            String newAlias = AliasInference.inferAliasWithPrefix("INTERSECT$", uniqueNameList);
            uniqueNameList.add(newAlias);
            return newAlias;
        }
    }

    private boolean isValidExpandCount(int count) {
        return count == 2 || count == 3;
    }
}
