package com.alibaba.graphscope.parallel.message;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public interface InMessageStore<I extends WritableComparable,
    M extends Writable> extends MessageStore<I,M>{

}
