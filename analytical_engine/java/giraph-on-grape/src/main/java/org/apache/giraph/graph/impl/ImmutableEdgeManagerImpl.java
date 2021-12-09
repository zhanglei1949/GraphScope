package org.apache.giraph.graph.impl;

import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.adaptor.AdjList;
import com.alibaba.graphscope.ds.adaptor.Nbr;
import com.alibaba.graphscope.fragment.SimpleFragment;
import com.alibaba.graphscope.utils.FFITypeFactoryhelper;
import com.alibaba.graphscope.utils.WritableFactory;
import java.util.Iterator;
import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.graph.EdgeManager;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Edge management for immutable edgecurt fragment.
 * In all time, there shall be only one managerImpl, one iterable, but multiple iterator.
 *
 * @param <OID_T> vertex id type
 * @param <EDATA_T> edge data type
 */
public class ImmutableEdgeManagerImpl<OID_T extends WritableComparable, EDATA_T extends Writable> implements EdgeManager<OID_T, EDATA_T> {
    private static Logger logger = LoggerFactory.getLogger(ImmutableEdgeManagerImpl.class);

    private SimpleFragment fragment;
    private Vertex<Long> curGrapeVertex;

    /**
     * One edge manager keeps one iterable. Use setters to update it.
     */
    private EdgeIterable edgeIterable;

    public ImmutableEdgeManagerImpl(SimpleFragment fragment){
        this.fragment = fragment;
        curGrapeVertex = FFITypeFactoryhelper.newVertexLong();
        edgeIterable = new EdgeIterable();
    }

    /**
     * Get the number of outgoing edges on this vertex.
     *
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges() {
        return (int) fragment.getEdgeNum();
    }

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEdge()) during iteration leads to undefined behavior.
     *
     * @return the out edges (sort order determined by subclass implementation).
     */
    @Override
    public Iterable<Edge<OID_T, EDATA_T>> getEdges(long lid) {
        curGrapeVertex.SetValue(lid);
        return edgeIterable;
    }

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<OID_T, EDATA_T>> edges) {
        logger.error("Not implemented");
    }

    /**
     * Get an iterable of out-edges that can be modified in-place. This can mean changing the
     * current edge value or removing the current edge (by using the iterator version). Note:
     * accessing the edges with other methods (e.g., addEdge()) during iteration leads to undefined
     * behavior.
     *
     * @return An iterable of mutable out-edges
     */
    @Override
    public Iterable<MutableEdge<OID_T, EDATA_T>> getMutableEdges() {
        logger.error("Not implemented");
        return null;
    }

    /**
     * Return the value of the first edge with the given target vertex id, or null if there is no
     * such edge. Note: edge value objects returned by this method may be invalidated by the next
     * call. Thus, keeping a reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return EDATA_Tdge value (or null if missing)
     */
    @Override
    public EDATA_T getEdgeValue(OID_T targetVertexId) {
        return null;
    }

    /**
     * If an edge to the target vertex exists, set it to the given edge value. This only makes sense
     * with strict graphs.
     *
     * @param targetVertexId Target vertex id
     * @param edgeValue      EDATA_Tdge value
     */
    @Override
    public void setEdgeValue(OID_T targetVertexId, EDATA_T edgeValue) {
        logger.error("Not implemented");
    }

    /**
     * Get an iterable over the values of all edges with the given target vertex id. This only makes
     * sense for multigraphs (i.e. graphs with parallel edges). Note: edge value objects returned by
     * this method may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge value almost always leads to undesired behavior.
     *
     * @param targetVertexId Target vertex id
     * @return Iterable of edge values
     */
    @Override
    public Iterable<EDATA_T> getAllEdgeValues(OID_T targetVertexId) {
        return null;
    }

    /**
     * Add an edge for this vertex (happens immediately)
     *
     * @param edge Edge to add
     */
    @Override
    public void addEdge(Edge<OID_T, EDATA_T> edge) {
        logger.error("Not implemented");
    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(OID_T targetVertexId) {
        logger.error("Not implemented");
    }

//    /**
//     * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator, copy any remaining
//     * edges to the new {@link OutEdges} data structure and keep a direct reference to it (thus
//     * discarding the wrapper). Called by the Giraph infrastructure after computation.
//     */
    @Override
    public void unwrapMutableEdges() {
        logger.error("Not implemented");
    }

    /**
     * Iterable for edges from one vertex.
     */
    public class EdgeIterable implements  Iterable<Edge<OID_T,EDATA_T>>{
        public EdgeIterable(){ }

        /**
         * Returns an iterator over elements of type {@code T}.
         * Make sure lid is updated before calling iterator()
         *
         * @return an Iterator.
         */
        @Override
        public Iterator<Edge<OID_T, EDATA_T>> iterator() {
            return new Iterator<Edge<OID_T, EDATA_T>>() {
                /**
                 * A reusable edge.
                 */
                private ReusableEdge<OID_T,EDATA_T> edge;
                private Iterator<Nbr> nbrIterator;
                private Nbr nbr;
                {
                    this.edge = new DefaultEdge<>();
                    this.nbrIterator = fragment.getOutgoingAdjList(curGrapeVertex).iterator().iterator();
                    edge.setTargetVertexId((OID_T) WritableFactory.newOid());
                    edge.setValue((EDATA_T) WritableFactory.newEData());
                }

                @Override
                public boolean hasNext() {
                    return iterator().hasNext();
                }

                @Override
                public Edge<OID_T, EDATA_T> next() {
                    nbr = nbrIterator.next();
                    //TODO: resolve this
                    if (edge.getTargetVertexId() instanceof LongWritable){
                        ((LongWritable) edge.getTargetVertexId()).set(
                            (Long) nbr.neighbor().GetValue());
                    }
                    else {
                        logger.error("Current not supported: " + WritableFactory.getOidClassName());
                    }

                    if (edge.getValue() instanceof DoubleWritable){
                        ((DoubleWritable) edge.getValue()).set((Double) nbr.data());
                    }
                    else {
                        logger.error("Current not supported: " + WritableFactory.getEdataClassName());
                    }
                    return edge;
                }
            };
        }
    }
}
