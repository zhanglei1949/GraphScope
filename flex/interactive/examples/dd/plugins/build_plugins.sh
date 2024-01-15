ARCH=`uname -m`
#FLEX_HOME=/usr/local
FLEX_HOME=/home/graphscope/flex_home/
#FLEX_HOME=/home/graphscope/flex_home/

#for i in query0 query1 query2 query3
for i in query1
do
  g++ -flto -fPIC  -g -finline-functions -zopt --std=c++17  -I/usr/lib/${ARCH}-linux-gnu/openmpi/include -I${FLEX_HOME}/include -L${FLEX_HOME}/lib -rdynamic -O3 -o lib${i}.so ${i}.cc -lflex_utils -lflex_rt_mutable_graph -lflex_graph_db -shared
done
