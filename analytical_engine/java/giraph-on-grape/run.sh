
export MAX_SUPER_STEP=10
export MESSAGE_MANAGER_TYPE=netty
GLOG_v=10 mpirun -envlist GLOG_v,GRAPE_JVM_OPTS,USER_JAR_PATH,GRAPH_TYPE,APP_TYPE -n 4 -f ~/hostfile ./giraph_runner --app_class com.alibaba.graphscope.samples.MessageBenchMark  --efile ~/libgrape-lite/dataset/p2p-31.e --vfile ~/libgrape-lite/dataset/p2p-31.v --worker_context_class com.alibaba.graphscope.samples.MessageBenchMark\$MessageBenchMarkWorkerContext --lib_path /opt/graphscope/lib/libgiraph-jni.so --loading_thread_num 1