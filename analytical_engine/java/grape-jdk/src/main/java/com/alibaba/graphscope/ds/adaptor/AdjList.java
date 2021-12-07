package com.alibaba.graphscope.ds.adaptor;

import com.alibaba.graphscope.ds.GrapeNbr;
import java.util.Iterator;

public interface AdjList<VID_T, EDATA_T> {

    String type();

    /**
     * Get the begin Nbr.
     *
     * @return the first Nbr.
     */
    Nbr<VID_T, EDATA_T> begin();

    /**
     * Get the last Nbr.
     *
     * @return the last Nbr.
     */
    Nbr<VID_T, EDATA_T> end();

    /**
     * Get the size of this adjList.
     *
     * @return size
     */
    long size();

    /**
     * The iterator for adjlist. You can use enhanced for loop instead of directly using this.
     *
     * @return the iterator.
     */
    default Iterable<Nbr<VID_T, EDATA_T>> iterator() {
        if (type().equals(GrapeAdjListAdaptor.TYPE)) {
            return () ->
                    new Iterator<Nbr<VID_T, EDATA_T>>() {
                        GrapeNbr<VID_T, EDATA_T> beginPtr = (GrapeNbr<VID_T, EDATA_T>) begin();
                        GrapeNbr<VID_T, EDATA_T> endPtr = (GrapeNbr<VID_T, EDATA_T>) end();
                        GrapeNbrAdaptor<VID_T, EDATA_T> curPtr = new GrapeNbrAdaptor<>(beginPtr);
                        long currentAddress;
                        long endAddress;
                        long elementSize;

                        {
                            this.currentAddress = beginPtr.getAddress();
                            this.endAddress = endPtr.getAddress();
                            this.elementSize = beginPtr.elementSize();
                        }

                        public boolean hasNext() {
                            return this.currentAddress != this.endAddress;
                        }

                        public Nbr<VID_T, EDATA_T> next() {
                            //                    beginPtr.moveToV(this.currentAddress);
                            curPtr.setAddress(this.currentAddress);
                            this.currentAddress += this.elementSize;
                            return curPtr;
                        }
                    };
        } else if (type().equals(ProjectedAdjListAdaptor.TYPE)) {
            return () ->
                    new Iterator<Nbr<VID_T, EDATA_T>>() {
                        //                void RuntimeException("Not implemented");
                        //                Nbr<VID_T, EDATA_T> begin = begin().dec();
                        //                ProjectedNbr<VID_T,EDATA_T>  begin =
                        // (ProjectedNbr<VID_T,EDATA_T>) begin().dec();
                        //                ProjectedNbr<VID_T,EDATA_T>  end =
                        // (ProjectedNbr<VID_T,EDATA_T>) end();
                        //
                        //                Nbr<VID_T, EDATA_T> curPtr = new
                        // ProjectedNbrAdaptor<VID_T,EDATA_T>(begin);
                        //                boolean flag = false;
                        //
                        @Override
                        public boolean hasNext() {
                            return false;
                            //                    if (!flag) {
                            //                        curPtr = curPtr.inc();
                            //                        flag = !curPtr.eq(end);
                            //                    }
                            //                    return flag;
                        }
                        //
                        @Override
                        public Nbr<VID_T, EDATA_T> next() {
                            //                    flag = false;
                            //                    return cur;
                            return null;
                        }
                    };
        }
        return null;
    }
}
