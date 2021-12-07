package org.apache.giraph.graph;

import com.alibaba.graphscope.parallel.GiraphMessageManager;
import com.alibaba.graphscope.parallel.impl.GiraphDefaultMessageManager;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.math.ode.ODEIntegrator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.graph.impl.CommunicatorImpl;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Implement all methods in Computation other than compute, which left for user to define.
 * @param <OID_T> original vertex id.
 * @param <VDATA_T> vertex data type.
 * @param <EDATA_T> edata type.
 * @param <IN_MSG_T> incoming msg type.
 * @param <OUT_MSG_T> outgoing msg type.
 */
public abstract class AbstractComputation<OID_T extends WritableComparable,
    VDATA_T extends Writable,
    EDATA_T extends Writable,
    IN_MSG_T extends Writable,
    OUT_MSG_T extends Writable> extends CommunicatorImpl implements Computation<OID_T,VDATA_T,EDATA_T,IN_MSG_T,OUT_MSG_T>{

    private GiraphMessageManager<OID_T,VDATA_T,EDATA_T, IN_MSG_T,OUT_MSG_T> giraphMessageManager;

    public void setGiraphMessageManager(GiraphMessageManager<OID_T,VDATA_T,EDATA_T,IN_MSG_T,OUT_MSG_T> giraphMessageManager){
        this.giraphMessageManager = giraphMessageManager;
    }

    /**
     * Prepare for computation. This method is executed exactly once prior to {@link
     * #compute(Vertex, Iterable)} being called for any of the vertices in the partition.
     */
    @Override
    public void preSuperstep() {

    }

    /**
     * Finish computation. This method is executed exactly once after computation for all vertices
     * in the partition is complete.
     */
    @Override
    public void postSuperstep() {

    }

    /**
     * Retrieves the current superstep.
     *
     * @return Current superstep
     */
    @Override
    public long getSuperstep() {
        return 0;
    }

    /**
     * Get the total (all workers) number of vertices that existed in the previous superstep.
     *
     * @return Total number of vertices (-1 if first superstep)
     */
    @Override
    public long getTotalNumVertices() {
        return 0;
    }

    /**
     * Get the total (all workers) number of edges that existed in the previous superstep.
     *
     * @return Total number of edges (-1 if first superstep)
     */
    @Override
    public long getTotalNumEdges() {
        return 0;
    }

    /**
     * Get the mapper context
     *
     * @return Mapper context
     */
    @Override
    public Context getContext() {
        return null;
    }

    /**
     * Get the worker context
     *
     * @return WorkerContext context
     */
    @Override
    public WorkerContext getWorkerContext() {
        return null;
    }

    @Override
    public void addVertexRequest(OID_T id, VDATA_T value, OutEdges<OID_T, EDATA_T> edges)
        throws IOException {

    }

    @Override
    public void addVertexRequest(OID_T id, VDATA_T value) throws IOException {

    }

    @Override
    public void removeVertexRequest(OID_T vertexId) throws IOException {

    }

    @Override
    public void addEdgeRequest(OID_T sourceVertexId, Edge<OID_T,EDATA_T> edge) throws IOException {

    }

    @Override
    public void removeEdgesRequest(OID_T sourceVertexId,
        OID_T targetVertexId) throws IOException {

    }

    @Override
    public void sendMessage(OID_T id, OUT_MSG_T message) {

    }

    @Override
    public void sendMessageToAllEdges(Vertex<OID_T,VDATA_T,EDATA_T> vertex, OUT_MSG_T message) {
        giraphMessageManager.sendMessageToAllEdges(vertex, message);
    }

    @Override
    public void sendMessageToMultipleEdges(Iterator<OID_T> vertexIdIterator, OUT_MSG_T message) {

    }

    /**
     * Get number of workers
     *
     * @return Number of workers
     */
    @Override
    public int getWorkerCount() {
        return 0;
    }

    /**
     * Get index for this worker
     *
     * @return Index of this worker
     */
    @Override
    public int getMyWorkerIndex() {
        return 0;
    }

    /**
     * Get worker index which will contain vertex with given id, if such vertex exists.
     *
     * @param vertexId vertex id
     * @return worker index
     */
    @Override
    public int getWorkerForVertex(OID_T vertexId) {
        return 0;
    }
}
