package com.alibaba.graphscope.parallel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Writable;

/**
 * An iterable instances holding msgs for a vertex.
 * @param <MSG_T> msg type.
 */
public class MessageIterable<MSG_T extends Writable> implements Iterable<MSG_T> {
    public static int DEFAULT_MESSAGE_ITERABLE_SIZE = 16;

    private List<MSG_T> msgs;

    public MessageIterable(){
        msgs = new ArrayList<>(DEFAULT_MESSAGE_ITERABLE_SIZE);
    }

    /**
     * Returns an iterator over elements of type {@code T}.
     *
     * @return an Iterator.
     */
    @Override
    public Iterator<MSG_T> iterator() {
        return msgs.iterator();
    }


    public void append(MSG_T msg){
        msgs.add(msg);
    }

    public void clear(){
        msgs.clear();
    }
}
