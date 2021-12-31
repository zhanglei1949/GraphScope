package com.alibaba.graphscope.parallel.message;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Base interface defining message store.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageStore<I extends WritableComparable,
    M extends Writable> {

    /**
     * Gets messages for a vertex.  The lifetime of every message is only
     * guaranteed until the iterator's next() method is called. Do not hold
     * references to objects returned by this iterator.
     *
     * @param vertexId Vertex id for which we want to get messages
     * @return Iterable of messages for a vertex id
     */
    Iterable<M> getVertexMessages(I vertexId);

    /**
     * Check if we have messages for some vertex
     *
     * @param vertexId Id of vertex which we want to check
     * @return True iff we have messages for vertex with required id
     */
    boolean hasMessagesForVertex(I vertexId);

    /**
     * Adds a message for a particular vertex
     * The method is used by InternalMessageStore to send local messages; for
     * the general case, use a more efficient addPartitionMessages
     *
     * @param vertexId Id of target vertex
     * @param message  A message to send
     * @throws IOException
     */
    void addMessage(I vertexId, M message) throws IOException;

}
