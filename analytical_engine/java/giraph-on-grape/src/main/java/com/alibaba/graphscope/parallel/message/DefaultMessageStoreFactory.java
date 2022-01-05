package com.alibaba.graphscope.parallel.message;

import com.alibaba.graphscope.fragment.SimpleFragment;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.conf.MessageClasses;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DefaultMessageStoreFactory<I extends WritableComparable,
    M extends Writable, GS_VID_T> implements MessageStoreFactory<I,M, MessageStore<I,M,GS_VID_T>>{
    private SimpleFragment<?,GS_VID_T,?,?> fragment;
    private ImmutableClassesGiraphConfiguration<I,?,?> conf;
    /**
     * Creates new message store.
     *
     * @param messageClasses Message classes information to be held in the store
     * @return New message store
     */
    @Override
    public MessageStore<I, M,GS_VID_T> newStore(MessageClasses<I, M> messageClasses) {
        return new SimpleMessageStore<>(fragment, conf);
    }

    /**
     * Implementation class should use this method of initialization of any required internal
     * state.
     *
     * @param fragment fragment used for partition querying
     * @param conf     Configuration
     */
    @Override
    public void initialize(SimpleFragment fragment,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
        this.fragment = fragment;
        this.conf = conf;
    }
}
