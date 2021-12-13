package org.apache.giraph.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.impl.DefaultWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationUtils {

    private static Logger logger = LoggerFactory.getLogger(ConfigurationUtils.class);

    public static final String APP_CLASS_STR = "app_class";
    public static final String WORKER_CONTEXT_CLASS_STR = "worker_context_class";

    public static final String VERTEX_INPUT_FORMAT_CLASS_STR = "input_format_class";

    /**
     * Translate CLI arguments to GiraphRunner into Configuration Key-Value pairs.
     *
     * @param giraphConfiguration configuration to set.
     * @param jsonObject          input json params
     */
    public static void parseArgs(final GiraphConfiguration giraphConfiguration,
        JSONObject jsonObject)
        throws ClassNotFoundException {
        if (!jsonObject.getString(WORKER_CONTEXT_CLASS_STR).isEmpty()) {
            giraphConfiguration.setWorkerContextClass(
                (Class<? extends WorkerContext>) Class
                    .forName(jsonObject.getString(WORKER_CONTEXT_CLASS_STR))
            );
            logger.info("Setting worker context class: " + jsonObject.getString(WORKER_CONTEXT_CLASS_STR));
        } else {
            //set the default worker context class
            giraphConfiguration.setWorkerContextClass(DefaultWorkerContext.class);
            logger.info("Setting worker context class: " + DefaultWorkerContext.class.getName());
        }

        if (!jsonObject.getString(APP_CLASS_STR).isEmpty()) {
            giraphConfiguration.setComputationClass(
                (Class<? extends AbstractComputation>) Class
                    .forName(jsonObject.getString(APP_CLASS_STR))
            );
            logger.info("Setting app class: " + jsonObject.getString(APP_CLASS_STR));
        }else {
            logger.error("No computation class defined");
        }

        if (!jsonObject.getString(VERTEX_INPUT_FORMAT_CLASS_STR).isEmpty()) {
            giraphConfiguration.setVertexInputFormatClass(
                (Class<? extends VertexInputFormat>) Class
                    .forName(jsonObject.getString(VERTEX_INPUT_FORMAT_CLASS_STR))
            );
            logger.info("Setting vertex input format class: " + jsonObject.getString(VERTEX_INPUT_FORMAT_CLASS_STR));
        }
        else {
            logger.info("No vertex input class found, using default one.");
        }
    }
}
