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

package com.alibaba.graphscope.common.ir.planner.rules;

import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphPhysicalExpand;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class DegreeFusionRule<C extends RelRule.Config> extends RelRule<C>
        implements TransformationRule {
    protected DegreeFusionRule(C config) {
        super(config);
    }

    protected RelNode transform(
            GraphLogicalAggregate count, GraphLogicalExpand expand, GraphBuilder builder) {
        RelNode expandDegree =
                GraphPhysicalExpand.create(
                        (GraphOptCluster) expand.getCluster(),
                        ImmutableList.of(),
                        expand.getInput(0),
                        expand,
                        null,
                        GraphOpt.PhysicalExpandOpt.DEGREE,
                        null);
        builder.push(expandDegree);
        Preconditions.checkArgument(
                !count.getAggCalls().isEmpty(),
                "there should be at least one aggregate call in count");
        String countAlias = count.getAggCalls().get(0).getAlias();
        return builder.aggregate(
                        builder.groupKey(),
                        builder.sum0(false, countAlias, builder.variable((String) null)))
                .build();
    }

    // transform expand + count to expandDegree + sum
    public static class ExpandDegreeFusionRule
            extends DegreeFusionRule<ExpandDegreeFusionRule.Config> {
        protected ExpandDegreeFusionRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalAggregate count = call.rel(0);
            GraphLogicalExpand expand = call.rel(1);
            GraphBuilder builder = (GraphBuilder) call.builder();
            call.transformTo(transform(count, expand, builder));
        }

        public static class Config implements RelRule.Config {
            public static ExpandDegreeFusionRule.Config DEFAULT =
                    new ExpandDegreeFusionRule.Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalAggregate.class)
                                                    .predicate(
                                                            (GraphLogicalAggregate aggregate) -> {
                                                                RelBuilder.GroupKey key =
                                                                        aggregate.getGroupKey();
                                                                List<GraphAggCall> calls =
                                                                        aggregate.getAggCalls();
                                                                return key.groupKeyCount() == 0
                                                                        && calls.size() == 1
                                                                        && calls.get(0)
                                                                                        .getAggFunction()
                                                                                        .getKind()
                                                                                == SqlKind.COUNT
                                                                        && !calls.get(0)
                                                                                .isDistinct();
                                                            })
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(
                                                                                    GraphLogicalExpand
                                                                                            .class)
                                                                            .anyInputs()));
            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public ExpandDegreeFusionRule.Config withRelBuilderFactory(
                    RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public ExpandDegreeFusionRule.Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public ExpandDegreeFusionRule.Config withOperandSupplier(
                    OperandTransform operandTransform) {
                this.operandSupplier = operandTransform;
                return this;
            }

            @Override
            public OperandTransform operandSupplier() {
                return this.operandSupplier;
            }

            @Override
            public @org.checkerframework.checker.nullness.qual.Nullable String description() {
                return this.description;
            }

            @Override
            public ExpandDegreeFusionRule toRule() {
                return new ExpandDegreeFusionRule(this);
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }

    // transform expand + getV + count to expandDegree + sum
    public static class ExpandGetVDegreeFusionRule
            extends DegreeFusionRule<ExpandGetVDegreeFusionRule.Config> {
        protected ExpandGetVDegreeFusionRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalAggregate count = call.rel(0);
            GraphLogicalExpand expand = call.rel(2);
            GraphBuilder builder = (GraphBuilder) call.builder();
            call.transformTo(transform(count, expand, builder));
        }

        public static class Config implements RelRule.Config {
            public static ExpandGetVDegreeFusionRule.Config DEFAULT =
                    new ExpandGetVDegreeFusionRule.Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalAggregate.class)
                                                    // should be global count and is not distinct
                                                    .predicate(
                                                            (GraphLogicalAggregate aggregate) -> {
                                                                RelBuilder.GroupKey key =
                                                                        aggregate.getGroupKey();
                                                                List<GraphAggCall> calls =
                                                                        aggregate.getAggCalls();
                                                                return key.groupKeyCount() == 0
                                                                        && calls.size() == 1
                                                                        && calls.get(0)
                                                                                        .getAggFunction()
                                                                                        .getKind()
                                                                                == SqlKind.COUNT
                                                                        && !calls.get(0)
                                                                                .isDistinct();
                                                            })
                                                    .oneInput(
                                                            b1 ->
                                                                    // should be getV which opt is
                                                                    // not BOTH and without any
                                                                    // filters
                                                                    b1.operand(
                                                                                    GraphLogicalGetV
                                                                                            .class)
                                                                            .predicate(
                                                                                    (GraphLogicalGetV
                                                                                                    getV) ->
                                                                                            getV
                                                                                                                    .getOpt()
                                                                                                            != GraphOpt
                                                                                                                    .GetV
                                                                                                                    .BOTH
                                                                                                    && ObjectUtils
                                                                                                            .isEmpty(
                                                                                                                    getV
                                                                                                                            .getFilters()))
                                                                            .oneInput(
                                                                                    b2 ->
                                                                                            b2.operand(
                                                                                                            GraphLogicalExpand
                                                                                                                    .class)
                                                                                                    .anyInputs())));
            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public ExpandGetVDegreeFusionRule.Config withRelBuilderFactory(
                    RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public ExpandGetVDegreeFusionRule.Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public ExpandGetVDegreeFusionRule.Config withOperandSupplier(
                    OperandTransform operandTransform) {
                this.operandSupplier = operandTransform;
                return this;
            }

            @Override
            public OperandTransform operandSupplier() {
                return this.operandSupplier;
            }

            @Override
            public @org.checkerframework.checker.nullness.qual.Nullable String description() {
                return this.description;
            }

            @Override
            public ExpandGetVDegreeFusionRule toRule() {
                return new ExpandGetVDegreeFusionRule(this);
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }

    // transform expand + getV + groupCount to expandDegree
    public static class ExpandGetVDegreeFusionRule2
            extends DegreeFusionRule<ExpandGetVDegreeFusionRule2.Config> {
        protected ExpandGetVDegreeFusionRule2(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            GraphLogicalAggregate count = call.rel(0);
            GraphLogicalExpand expand = call.rel(2);
            GraphLogicalAggregate dedup = call.rel(3);
            GraphBuilder builder = (GraphBuilder) call.builder();
            call.transformTo(transform(count, expand, dedup, builder));
        }

        private RelNode transform(
                GraphLogicalAggregate count,
                GraphLogicalExpand expand,
                GraphLogicalAggregate dedup,
                GraphBuilder builder) {
            int groupKeyCount = count.getGroupKey().groupKeyCount();
            if (groupKeyCount != 1) return count;
            RexGraphVariable key = (RexGraphVariable) count.getGroupKey().getVariables().get(0);
            if (key.getProperty() != null
                    || key.getAliasId() != expand.getStartAlias().getAliasId()) {
                return count;
            }
            if (!dedupByKey(dedup, key.getAliasId())) {
                return count;
            }
            Preconditions.checkArgument(
                    !count.getAggCalls().isEmpty(),
                    "there should be at least one aggregate call in count");
            String countAlias = count.getAggCalls().get(0).getAlias();
            RelNode expandDegree =
                    GraphPhysicalExpand.create(
                            expand.getCluster(),
                            ImmutableList.of(),
                            expand.getInput(0),
                            expand,
                            null,
                            GraphOpt.PhysicalExpandOpt.DEGREE,
                            countAlias);
            builder.push(expandDegree);
            return builder.project(
                            ImmutableList.of(
                                    builder.variable(expand.getStartAlias().getAliasName()),
                                    builder.variable(countAlias)))
                    .build();
        }

        private boolean dedupByKey(GraphLogicalAggregate dedup, int keyAliasId) {
            if (dedup.getAggCalls().isEmpty() && dedup.getGroupKey().groupKeyCount() == 1) {
                RexGraphVariable key = (RexGraphVariable) dedup.getGroupKey().getVariables().get(0);
                return key.getAliasId() == keyAliasId;
            }
            return false;
        }

        public static class Config implements RelRule.Config {
            public static ExpandGetVDegreeFusionRule2.Config DEFAULT =
                    new ExpandGetVDegreeFusionRule2.Config()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(GraphLogicalAggregate.class)
                                                    // should be global count and is not distinct
                                                    .predicate(
                                                            (GraphLogicalAggregate aggregate) -> {
                                                                List<GraphAggCall> calls =
                                                                        aggregate.getAggCalls();
                                                                return calls.size() == 1
                                                                        && calls.get(0)
                                                                                        .getAggFunction()
                                                                                        .getKind()
                                                                                == SqlKind.COUNT
                                                                        && !calls.get(0)
                                                                                .isDistinct();
                                                            })
                                                    .oneInput(
                                                            b1 ->
                                                                    // should be getV which opt is
                                                                    // not BOTH and without any
                                                                    // filters
                                                                    b1.operand(
                                                                                    GraphLogicalGetV
                                                                                            .class)
                                                                            .predicate(
                                                                                    (GraphLogicalGetV
                                                                                                    getV) ->
                                                                                            getV
                                                                                                                    .getOpt()
                                                                                                            != GraphOpt
                                                                                                                    .GetV
                                                                                                                    .BOTH
                                                                                                    && ObjectUtils
                                                                                                            .isEmpty(
                                                                                                                    getV
                                                                                                                            .getFilters()))
                                                                            .oneInput(
                                                                                    b2 ->
                                                                                            b2.operand(
                                                                                                            GraphLogicalExpand
                                                                                                                    .class)
                                                                                                    .oneInput(
                                                                                                            b3 ->
                                                                                                                    b3.operand(
                                                                                                                                    GraphLogicalAggregate
                                                                                                                                            .class)
                                                                                                                            .anyInputs()))));
            private RelRule.OperandTransform operandSupplier;
            private @Nullable String description;
            private RelBuilderFactory builderFactory;

            @Override
            public ExpandGetVDegreeFusionRule2.Config withRelBuilderFactory(
                    RelBuilderFactory relBuilderFactory) {
                this.builderFactory = relBuilderFactory;
                return this;
            }

            @Override
            public ExpandGetVDegreeFusionRule2.Config withDescription(
                    @org.checkerframework.checker.nullness.qual.Nullable String s) {
                this.description = s;
                return this;
            }

            @Override
            public ExpandGetVDegreeFusionRule2.Config withOperandSupplier(
                    OperandTransform operandTransform) {
                this.operandSupplier = operandTransform;
                return this;
            }

            @Override
            public OperandTransform operandSupplier() {
                return this.operandSupplier;
            }

            @Override
            public @org.checkerframework.checker.nullness.qual.Nullable String description() {
                return this.description;
            }

            @Override
            public ExpandGetVDegreeFusionRule2 toRule() {
                return new ExpandGetVDegreeFusionRule2(this);
            }

            @Override
            public RelBuilderFactory relBuilderFactory() {
                return this.builderFactory;
            }
        }
    }
}
