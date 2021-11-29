package org.apache.hadoop.io;

import java.util.Comparator;

public interface RawComparator<T> extends Comparator<T> {
    int compare(byte[] var1, int var2, int var3, byte[] var4, int var5, int var6);
}
