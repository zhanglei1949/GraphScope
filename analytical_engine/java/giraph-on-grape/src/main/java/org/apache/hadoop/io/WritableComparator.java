//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.io;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class WritableComparator implements RawComparator {
    private static HashMap<Class, WritableComparator> comparators = new HashMap();
    private final Class<? extends WritableComparable> keyClass;
    private final WritableComparable key1;
    private final WritableComparable key2;
    private final DataInputBuffer buffer;

    public static synchronized WritableComparator get(Class<? extends WritableComparable> c) {
        WritableComparator comparator = (WritableComparator) comparators.get(c);
        if (comparator == null) {
            comparator = new WritableComparator(c, true);
        }

        return comparator;
    }

    public static synchronized void define(Class c, WritableComparator comparator) {
        comparators.put(c, comparator);
    }

    protected WritableComparator(Class<? extends WritableComparable> keyClass) {
        this(keyClass, false);
    }

    protected WritableComparator(
            Class<? extends WritableComparable> keyClass, boolean createInstances) {
        this.keyClass = keyClass;
        if (createInstances) {
            this.key1 = this.newKey();
            this.key2 = this.newKey();
            this.buffer = new DataInputBuffer();
        } else {
            this.key1 = this.key2 = null;
            this.buffer = null;
        }
    }

    public Class<? extends WritableComparable> getKeyClass() {
        return this.keyClass;
    }

    public WritableComparable newKey() {
        return (WritableComparable)
                ReflectionUtils.newInstance(this.keyClass, (Configuration) null);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        try {
            this.buffer.reset(b1, s1, l1);
            this.key1.readFields(this.buffer);
            this.buffer.reset(b2, s2, l2);
            this.key2.readFields(this.buffer);
        } catch (IOException var8) {
            throw new RuntimeException(var8);
        }

        return this.compare(this.key1, this.key2);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }

    public int compare(Object a, Object b) {
        return this.compare((WritableComparable) a, (WritableComparable) b);
    }

    public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        int end1 = s1 + l1;
        int end2 = s2 + l2;
        int i = s1;

        for (int j = s2; i < end1 && j < end2; ++j) {
            int a = b1[i] & 255;
            int b = b2[j] & 255;
            if (a != b) {
                return a - b;
            }

            ++i;
        }

        return l1 - l2;
    }

    public static int hashBytes(byte[] bytes, int offset, int length) {
        int hash = 1;

        for (int i = offset; i < offset + length; ++i) {
            hash = 31 * hash + bytes[i];
        }

        return hash;
    }

    public static int hashBytes(byte[] bytes, int length) {
        return hashBytes(bytes, 0, length);
    }

    public static int readUnsignedShort(byte[] bytes, int start) {
        return ((bytes[start] & 255) << 8) + (bytes[start + 1] & 255);
    }

    public static int readInt(byte[] bytes, int start) {
        return ((bytes[start] & 255) << 24)
                + ((bytes[start + 1] & 255) << 16)
                + ((bytes[start + 2] & 255) << 8)
                + (bytes[start + 3] & 255);
    }

    public static float readFloat(byte[] bytes, int start) {
        return Float.intBitsToFloat(readInt(bytes, start));
    }

    public static long readLong(byte[] bytes, int start) {
        return ((long) readInt(bytes, start) << 32)
                + ((long) readInt(bytes, start + 4) & 4294967295L);
    }

    public static double readDouble(byte[] bytes, int start) {
        return Double.longBitsToDouble(readLong(bytes, start));
    }

    public static long readVLong(byte[] bytes, int start) throws IOException {
        int len = bytes[start];
        if (len >= -112) {
            return len;
        } else {
            boolean isNegative = len < -120;
            len = isNegative ? -(len + 120) : -(len + 112);
            if (start + 1 + len > bytes.length) {
                throw new IOException("Not enough number of bytes for a zero-compressed integer");
            } else {
                long i = 0L;

                for (int idx = 0; idx < len; ++idx) {
                    i <<= 8;
                    i |= (long) (bytes[start + 1 + idx] & 255);
                }

                return isNegative ? ~i : i;
            }
        }
    }

    public static int readVInt(byte[] bytes, int start) throws IOException {
        return (int) readVLong(bytes, start);
    }
}
