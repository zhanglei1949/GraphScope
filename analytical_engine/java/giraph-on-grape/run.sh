
export MAX_SUPER_STEP=10
export MESSAGE_MANAGER_TYPE=netty
export USER_JAR_PATH=/home/admin/gs/analytical_engine/java/giraph-on-grape/target/giraph-on-grape-shaded.jar
export OUT_MESSAGE_CACHE_TYPE=ByteBuf
export MESSAGE_STORE_TYPE=primitve
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
-n 2 \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE \
./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark \
--efile livejournal.e --vfile livejournal.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix livejournal

# com 
GLOG_v=10 mpirun \
-n 4 \
-f ~/hostfile \
-envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE,OUT_MESSAGE_CACHE_TYPE,MESSAGE_STORE_TYPE \
./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark \
--efile com.e --vfile com.v \
--worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext \
--lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1 \
--serialize true --serialize_prefix com



osscmd multiget --thread_num=16 oss://lei-benchdata/datagen-9_0-fb.v datagen.v

GLOG_v=10 mpirun -envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE,MAX_SUPER_STEP,MESSAGE_MANAGER_TYPE -n 4 ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  --efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v --worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext --lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1
# --efile ./datagen-9_0.e --vfile ./datagen-9_0.v \