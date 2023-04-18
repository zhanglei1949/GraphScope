package com.alibaba.graphscope.example.stringApp;

import static com.alibaba.graphscope.utils.CppClassName.GS_ARROW_PROJECTED_FRAGMENT_IMPL_STRING_TYPED_ARRAY;

import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.context.ParallelContextBase;
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.StringTypedArray;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.IFragment;
import com.alibaba.graphscope.parallel.ParallelEngine;
import com.alibaba.graphscope.parallel.ParallelMessageManager;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.grapscope.example.stringApp.BfsContext;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 对于用户逻辑实现的App类，用户需要实现ParallelAppBase接口，并且实现PEval和IncEval接口
 */
public class Bfs implements
    ParallelAppBase<StringView, Long, Long, Long, com.alibaba.grapscope.example.stringApp.BfsContext>,
    ParallelEngine {

    private static Logger logger = LoggerFactory.getLogger(Bfs.class);
    static final String STRING_VIEW_NAME = "vineyard::arrow_string_view";

    @Override
    public void PEval(IFragment<StringView, Long, Long, Long> graph,
        ParallelContextBase<StringView, Long, Long, Long> context,
        ParallelMessageManager messageManager) {
        com.alibaba.grapscope.example.stringApp.BfsContext ctx = (com.alibaba.grapscope.example.stringApp.BfsContext) context;
        Vertex<Long> vertex = FFITypeFactoryhelper.newVertexLong();
        StringView.Factory factory = FFITypeFactory.getFactory(
            StringView.class, STRING_VIEW_NAME
            );
        StringView stringView = factory.create();
        stringView.fromJavaString(ctx.sourceOid);
        String recopied = stringView.toJavaString();
        logger.info("oid : {}", recopied);
        boolean inThisFrag = graph.getInnerVertex(stringView, vertex);
        ctx.currentDepth = 1;
        if (inThisFrag) {
            ctx.partialResults.set(vertex, 0);
            AdjList<Long, Long> adjList = graph.getOutgoingAdjList(vertex);
            for (Nbr<Long, Long> nbr : adjList.iterable()) {
                Vertex<Long> neighbor = nbr.neighbor();
                if (ctx.partialResults.get(neighbor) == Integer.MAX_VALUE) {
                    ctx.partialResults.set(neighbor, 1);
                    if (graph.isOuterVertex(neighbor)) {
                        messageManager.syncStateOnOuterVertexNoMsg(graph, neighbor, 0);
                    } else {
                        ctx.currentInnerUpdated.set(neighbor);
                    }
                }
            }
        }
        messageManager.forceContinue();
    }

    @Override
    public void IncEval(IFragment<StringView, Long, Long, Long> graph,
        ParallelContextBase<StringView, Long, Long, Long> context,
        ParallelMessageManager messageManager) {
        com.alibaba.grapscope.example.stringApp.BfsContext ctx = (BfsContext) context;
        VertexRange<Long> innerVertices = graph.innerVertices();
        int nextDepth = ctx.currentDepth + 1;
        ctx.nextInnerUpdated.clear();

        BiConsumer<Vertex<Long>, EmptyType> receiveMsg =
            (vertex, msg) -> {
                if (ctx.partialResults.get(vertex) == Integer.MAX_VALUE) {
                    ctx.partialResults.set(vertex, ctx.currentDepth);
                    ctx.currentInnerUpdated.set(vertex);
                }
            };
        Supplier<EmptyType> msgSupplier = EmptyType.factory::create;
        messageManager.parallelProcess(
            graph, ctx.threadNum, ctx.executor, msgSupplier, receiveMsg
        );

        BiConsumer<Vertex<Long>, Integer> vertexIntegerBiConsumer =
            (cur, finalTid) -> {
                AdjList<Long, Long> adjList = graph.getOutgoingAdjList(cur);
                for (Nbr<Long, Long> nbr : adjList.iterable()) {
                    Vertex<Long> vertex = nbr.neighbor();
                    if (ctx.partialResults.get(vertex) == Integer.MAX_VALUE) {
                        ctx.partialResults.set(vertex, nextDepth);
                        if (graph.isOuterVertex(vertex)) {
                            messageManager.syncStateOnOuterVertexNoMsg(graph, vertex, finalTid
                            );
                        } else {
                            ctx.nextInnerUpdated.insert(vertex);
                        }
                    }
                }
            };

        forEachVertex(
            innerVertices,
            ctx.threadNum,
            ctx.executor,
            ctx.currentInnerUpdated,
            vertexIntegerBiConsumer
        );

        ctx.currentDepth = nextDepth;
        if (!ctx.nextInnerUpdated.empty()) {
            messageManager.forceContinue();
        }
        ctx.currentInnerUpdated.assign(ctx.nextInnerUpdated);
    }
}
