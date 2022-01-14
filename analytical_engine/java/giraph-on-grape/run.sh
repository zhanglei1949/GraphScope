
export MAX_SUPER_STEP=10
export MESSAGE_MANAGER_TYPE=netty
export USER_JAR_PATH=/home/admin/gs/analytical_engine/java/giraph-on-grape/target/giraph-on-grape-shaded.jar
source /opt/graphscope/conf/grape_jvm_opts
GLOG_v=10 mpirun \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE \
-n 4  ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  \
--efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix p2p

# datagen
GLOG_v=10 mpirun \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE \
-n 4  ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  \
--efile ./datagen-9_0.e --vfile ./datagen-9_0.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix datagen
-f ~/hostfile


GLOG_v=10 mpirun -envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE -n 4 ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  --efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v --worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext --lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1