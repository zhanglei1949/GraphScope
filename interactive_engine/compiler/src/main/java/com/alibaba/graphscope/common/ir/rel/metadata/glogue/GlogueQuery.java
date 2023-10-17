package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.javatuples.Pair;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.fuzzy.FuzzyPatternProcessor;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.fuzzy.FuzzyPatternProcessor.FuzzyInfo;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.FuzzyPatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.FuzzyPatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternMapping;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;

public class GlogueQuery {
    private Glogue glogue;
    private FuzzyPatternProcessor fuzzyPatternProcessor;

    protected GlogueQuery(Glogue glogue, GlogueSchema schema) {
        this.glogue = glogue;
        this.fuzzyPatternProcessor = new FuzzyPatternProcessor(schema);
    }

    // the query API for optimizer
    public Set<GlogueEdge> getInEdges(Pattern pattern) {
        if (fuzzyPatternProcessor.isFuzzyPattern(pattern)) {
            Pair<Pattern, FuzzyInfo> patternWithInfo = this.fuzzyPatternProcessor
                    .processFuzzyPatternAndGetFuzzyInfo(pattern);
            Set<GlogueEdge> inEdges = glogue.getGlogueInEdges(patternWithInfo.getValue0());
            return fuzzyPatternProcessor.processGlogueEdgesWithFuzzyInfo(inEdges, patternWithInfo.getValue1(), false);
        } else {
            return glogue.getInEdges(pattern);
        }
    }

    // return a pair of non-fuzzy pattern together with fuzzy info
    public Pair<Pattern, FuzzyInfo> processFuzzyPatternAndGetFuzzyInfo(Pattern pattern) {
        return fuzzyPatternProcessor.processFuzzyPatternAndGetFuzzyInfo(pattern);
    }


    // the query API for optimizer
    public Set<GlogueEdge> getOutEdges(Pattern pattern) {
        if (fuzzyPatternProcessor.isFuzzyPattern(pattern)) {
            Pair<Pattern, FuzzyInfo> patternWithInfo = this.fuzzyPatternProcessor
                    .processFuzzyPatternAndGetFuzzyInfo(pattern);
            Optional<Pair<Pattern, PatternMapping>> glogueVertexWithMapping = glogue
                    .getGlogueVertexWithMapping(patternWithInfo.getValue0());
            if (glogueVertexWithMapping.isPresent()) {
                patternWithInfo.getValue1().setPatternMapping(glogueVertexWithMapping.get().getValue1());
                Set<GlogueEdge> outEdges = glogue.getGlogueOutEdges(glogueVertexWithMapping.get().getValue0());
                return fuzzyPatternProcessor.processGlogueEdgesWithFuzzyInfo(outEdges, patternWithInfo.getValue1(), true);
            } else {
                throw new RuntimeException(
                        "pattern not found in glogue graph. queries pattern " + pattern);
            }
        } else {
            return glogue.getOutEdges(pattern);
        }
    }

    public Double getRowCount(Pattern pattern) {
        if (fuzzyPatternProcessor.isFuzzyPattern(pattern)) {
            Pair<Pattern, FuzzyInfo> patternWithInfo = this.fuzzyPatternProcessor
                    .processFuzzyPatternAndGetFuzzyInfo(pattern);

            Double count = glogue.getRowCount(patternWithInfo.getValue0());
            return fuzzyPatternProcessor.estimateCountWithFuzzyInfo(count, patternWithInfo.getValue1());
        } else {
            return glogue.getRowCount(pattern);
        }
    }

    @Override
    public String toString() {
        return glogue.toString();
    }

    public static void main(String[] args) {
        GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
        Glogue gl = new Glogue().create(g, 3);
        GlogueQuery gq = new GlogueQuery(gl, g);
        Pattern p = new Pattern();

        // p1 -> s0 <- p2 + p1 -> p2
        PatternVertex v0 = new SinglePatternVertex(1, 0);
        PatternVertex v1 = new SinglePatternVertex(0, 1);
        PatternVertex v2 = new SinglePatternVertex(0, 2);
        // p -> s
        EdgeTypeId e = new EdgeTypeId(0, 1, 1);
        // p -> p
        EdgeTypeId e1 = new EdgeTypeId(0, 0, 0);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v1, v0, e);
        p.addEdge(v2, v0, e);
        p.addEdge(v1, v2, e1);
        p.reordering();

        // p0 -> s2 <- p1 + p0 -> p1
        Pattern p2 = new Pattern();
        PatternVertex v00 = new SinglePatternVertex(0, 0);
        PatternVertex v11 = new SinglePatternVertex(0, 1);
        PatternVertex v22 = new SinglePatternVertex(1, 2);
        p2.addVertex(v00);
        p2.addVertex(v11);
        p2.addVertex(v22);
        p2.addEdge(v00, v22, e);
        p2.addEdge(v11, v22, e);
        p2.addEdge(v00, v11, e1);
        p2.reordering();

        System.out.println("Pattern: " + p);

        Double count = gq.getRowCount(p);
        System.out.println("estimated count: " + count);

        System.out.println("Pattern2: " + p2);

        Double count2 = gq.getRowCount(p2);
        System.out.println("estimated count: " + count2);

        // (person) -(knows, creates)-> (person, software)
        Pattern p3 = new Pattern();
        PatternVertex v000 = new SinglePatternVertex(0, 0);
        PatternVertex v111 = new FuzzyPatternVertex(Arrays.asList(0,1), 1);
        p3.addVertex(v000);
        p3.addVertex(v111);
        List<EdgeTypeId> eTypeIds000 = Arrays.asList(new EdgeTypeId(0, 0, 0), new EdgeTypeId(0, 1, 1));
        PatternEdge e000 = new FuzzyPatternEdge(v000, v111, eTypeIds000, 0);
        p3.addEdge(v000, v111, e000);
        p3.reordering();
        System.out.println("Pattern3: " + p3);

        Double count3 = gq.getRowCount(p3);
        System.out.println("estimated count: " + count3);

    }
}
