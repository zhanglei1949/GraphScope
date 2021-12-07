package org.apache.giraph.graph;

import com.alibaba.graphscope.fragment.SimpleFragment;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.MutableEdgesWrapper;
import org.apache.giraph.edge.OutEdges;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class VertexImpl<OID_T extends WritableComparable, VDATA_T extends Writable, EDATA_T extends Writable> extends
    DefaultImmutableClassesGiraphConfigurable<OID_T,VDATA_T,EDATA_T> implements Vertex<OID_T,VDATA_T,EDATA_T>{

    private SimpleFragment fragment;
    private long lid;

    public VertexImpl(SimpleFragment fragment){
        this.fragment = fragment;
        lid = -1; //set to a negative value to ensure set lid to be called later.
    }

    /**
     * Initialize id, value, and edges. This method (or the alternative form initialize(id, value))
     * must be called after instantiation, unless readFields() is called.
     *
     * @param id    Vertex id
     * @param value Vertex value
     * @param edges Iterable of edges
     */
    @Override
    public void initialize(OID_T id, VDATA_T value, Iterable<Edge<OID_T, EDATA_T>> edges) {

    }

    /**
     * Initialize id and value. Vertex edges will be empty. This method (or the alternative form
     * initialize(id, value, edges)) must be called after instantiation, unless readFields() is
     * called.
     *
     * @param id    Vertex id
     * @param value Vertex value
     */
    @Override
    public void initialize(OID_T id, VDATA_T value) {

    }

    /**
     * Get the vertex id.
     *
     * @return My vertex id.
     */
    @Override
    public OID_T getId() {
        return null;
    }

    /**
     * Get the vertex value (data stored with vertex)
     *
     * @return Vertex value
     */
    @Override
    public VDATA_T getValue() {
        return null;
    }

    /**
     * Set the vertex data (immediately visible in the computation)
     *
     * @param value Vertex data to be set
     */
    @Override
    public void setValue(VDATA_T value) {

    }

    /**
     * After this is called, the compute() code will no longer be called for this vertex unless a
     * message is sent to it.  Then the compute() code will be called once again until this function
     * is called.  The application finishes only when all vertices vote to halt.
     */
    @Override
    public void voteToHalt() {

    }

    /**
     * Get the number of outgoing edges on this vertex.
     *
     * @return the total number of outbound edges from this vertex
     */
    @Override
    public int getNumEdges() {
        return 0;
    }

    /**
     * Get a read-only view of the out-edges of this vertex. Note: edge objects returned by this
     * iterable may be invalidated as soon as the next element is requested. Thus, keeping a
     * reference to an edge almost always leads to undesired behavior. Accessing the edges with
     * other methods (e.g., addEDATA_Tdge()) during iteration leads to undefined behavior.
     *
     * @return the out edges (sort order determined by subclass implementation).
     */
    @Override
    public Iterable<Edge<OID_T, EDATA_T>> getEdges() {
        return null;
    }

    /**
     * Set the outgoing edges for this vertex.
     *
     * @param edges Iterable of edges
     */
    @Override
    public void setEdges(Iterable<Edge<OID_T, EDATA_T>> edges) {

    }

    /**
     * Get an iterable of out-edges that can be modified in-place. This can mean changing the
     * current edge value or removing the current edge (by using the iterator version). Note:
     * accessing the edges with other methods (e.g., addEDATA_Tdge()) during iteration leads to
     * undefined behavior.
     *
     * @return An iterable of mutable out-edges
     */
    @Override
    public Iterable<MutableEdge<OID_T, EDATA_T>> getMutableEdges() {
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
     * @param edge EDATA_Tdge to add
     */
    @Override
    public void addEdge(Edge<OID_T, EDATA_T> edge) {

    }

    /**
     * Removes all edges pointing to the given vertex id.
     *
     * @param targetVertexId the target vertex id
     */
    @Override
    public void removeEdges(OID_T targetVertexId) {

    }

    /**
     * If a {@link MutableEdgesWrapper} was used to provide a mutable iterator, copy any remaining
     * edges to the new {@link OutEdges} data structure and keep a direct reference to it (thus
     * discarding the wrapper). Called by the Giraph infrastructure after computation.
     */
    @Override
    public void unwrapMutableEdges() {

    }

    /**
     * Re-activate vertex if halted.
     */
    @Override
    public void wakeUp() {

    }

    /**
     * Is this vertex done?
     *
     * @return True if halted, false otherwise.
     */
    @Override
    public boolean isHalted() {
        return false;
    }

    //Methods we need to adapt to grape
    public void setLocalId(int lid) {
        this.lid = lid;
    }

    public long getLocalId() {
        return lid;
    }
}
