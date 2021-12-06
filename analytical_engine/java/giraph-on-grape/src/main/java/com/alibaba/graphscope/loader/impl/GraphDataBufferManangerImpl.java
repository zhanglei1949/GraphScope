package com.alibaba.graphscope.loader.impl;

import com.alibaba.graphscope.loader.GraphDataBufferManager;
import com.alibaba.graphscope.stdcxx.FFIByteVecVector;
import com.alibaba.graphscope.stdcxx.FFIIntVecVector;

public class GraphDataBufferManangerImpl implements GraphDataBufferManager {
    public GraphDataBufferManangerImpl(
        FFIByteVecVector vidBuffers, FFIIntVecVector vidOffsets,
        FFIByteVecVector vertexDataBuffers, FFIByteVecVector edgeSrcIdBuffers,
        FFIIntVecVector edgeSrcIdOffsets, FFIByteVecVector edgeDstIdBuffers,
        FFIIntVecVector edgeDstIdOffsets, FFIByteVecVector edgeDataBuffers){

    }
}
