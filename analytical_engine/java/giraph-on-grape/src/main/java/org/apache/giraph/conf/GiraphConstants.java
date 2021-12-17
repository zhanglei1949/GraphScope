package org.apache.giraph.conf;

import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.factories.DefaultMessageValueFactory;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.impl.VertexImpl;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.impl.DefaultWorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Constants used all over Giraph for configuration.
 */
// CHECKSTYLE: stop InterfaceIsTypeCheck
public interface GiraphConstants {

    /**
     * 1KB in bytes
     */
    int ONE_KB = 1024;
    /**
     * 1MB in bytes
     */
    int ONE_MB = 1024 * 1024;

    /**
     * VertexOutputFormat class
     */
    ClassConfOption<VertexOutputFormat> VERTEX_OUTPUT_FORMAT_CLASS =
        ClassConfOption.create("giraph.vertexOutputFormatClass", null,
            VertexOutputFormat.class, "VertexOutputFormat class");

    /**
     * VertexOutputFormat class
     */
    ClassConfOption<VertexInputFormat> VERTEX_INPUT_FORMAT_CLASS =
        ClassConfOption.create("giraph.vertexInputFormatClass", null,
            VertexInputFormat.class, "VertexInputFormat class");

    /**
     * vertexOutputFormat sub-directory In Giraph, the output is to hdfs, they use the parent
     * directory of default parent directory, and use sub directory as sub.
     * <p>
     * In our project, we deem this configuration as ABSOLUTE path.
     */
    StrConfOption VERTEX_OUTPUT_FORMAT_SUBDIR =
        new StrConfOption("giraph.vertex.output.subdir", "",
            "VertexOutputFormat sub-directory");
    StrConfOption VERTEX_OUTPUT_PATH = new StrConfOption("giraph.vertex.output.path", "",
        "vertex output path");

    /**
     * Vertex index class
     */
    ClassConfOption<WritableComparable> VERTEX_ID_CLASS =
        ClassConfOption.create("giraph.vertexIdClass", null,
            WritableComparable.class, "Vertex index class");

    /**
     * Vertex value class
     */
    ClassConfOption<Writable> VERTEX_VALUE_CLASS =
        ClassConfOption.create("giraph.vertexValueClass", null, Writable.class,
            "Vertex value class");

    /**
     * Edge value class
     */
    ClassConfOption<Writable> EDGE_VALUE_CLASS =
        ClassConfOption.create("giraph.edgeValueClass", null, Writable.class,
            "Edge value class");

    /**
     * Vertex class
     */
    ClassConfOption<Vertex> VERTEX_CLASS =
        ClassConfOption.create("giraph.vertexClass",
            VertexImpl.class, Vertex.class,
            "Vertex class");

    /**
     * Outgoing message value class
     */
    ClassConfOption<Writable> OUTGOING_MESSAGE_VALUE_CLASS =
        ClassConfOption.create("giraph.outgoingMessageValueClass", null,
            Writable.class, "Outgoing message value class");

    /**
     * incoming message value class
     */
    ClassConfOption<Writable> INCOMING_MESSAGE_VALUE_CLASS =
        ClassConfOption.create("giraph.incomingMessageValueClass", null,
            Writable.class, "Outgoing message value class");

    /**
     * Worker context class
     */
    ClassConfOption<WorkerContext> WORKER_CONTEXT_CLASS =
        ClassConfOption.create("giraph.workerContextClass",
            DefaultWorkerContext.class, WorkerContext.class,
            "Worker contextclass");

    /**
     * Worker context class
     */
    ClassConfOption<AbstractComputation> COMPUTATION_CLASS =
        ClassConfOption.create("giraph.computationClass",
            null, AbstractComputation.class,
            "User computation class");

    /**
     * TypesHolder, used if Computation not set - optional
     */
    ClassConfOption<TypesHolder> TYPES_HOLDER_CLASS =
        ClassConfOption.create("giraph.typesHolder", null,
            TypesHolder.class,
            "TypesHolder, used if Computation not set - optional");

    /**
     * Message combiner class - optional
     */
    ClassConfOption<MessageCombiner> MESSAGE_COMBINER_CLASS =
        ClassConfOption.create("giraph.messageCombinerClass", null,
            MessageCombiner.class, "Message combiner class - optional");

    /**
     * Outgoing message value factory class - optional
     */
    ClassConfOption<MessageValueFactory>
        OUTGOING_MESSAGE_VALUE_FACTORY_CLASS =
        ClassConfOption.create("giraph.outgoingMessageValueFactoryClass",
            DefaultMessageValueFactory.class, MessageValueFactory.class,
            "Outgoing message value factory class - optional");

    /**
     * Class for Master - optional
     */
    ClassConfOption<MasterCompute> MASTER_COMPUTE_CLASS =
        ClassConfOption.create("giraph.masterComputeClass",
            DefaultMasterCompute.class, MasterCompute.class,
            "Class for Master - optional");

}
