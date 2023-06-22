FROM hqps-server-base:latest

RUN git clone https://github.com/zhanglei1949/GraphScope.git -b hqps-flex --single-branch && cd GraphScope/flex && \
    mkdir build && cd build && cmake .. && make -j

RUN cd GraphScope/interactive_engine/compiler && make build

ENV GIE_HOME=$HOME/GraphScope/interactive_engine/