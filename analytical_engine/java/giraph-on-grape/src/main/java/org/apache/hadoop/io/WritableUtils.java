//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hadoop.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public final class WritableUtils {
    public WritableUtils() {}

    public static byte[] readCompressedByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        } else {
            byte[] buffer = new byte[length];
            in.readFully(buffer);
            GZIPInputStream gzi =
                    new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer.length));
            byte[] outbuf = new byte[length];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            int len;
            while ((len = gzi.read(outbuf, 0, outbuf.length)) != -1) {
                bos.write(outbuf, 0, len);
            }

            byte[] decompressed = bos.toByteArray();
            bos.close();
            gzi.close();
            return decompressed;
        }
    }

    public static void skipCompressedByteArray(DataInput in) throws IOException {
        int length = in.readInt();
        if (length != -1) {
            skipFully(in, length);
        }
    }

    public static int writeCompressedByteArray(DataOutput out, byte[] bytes) throws IOException {
        if (bytes != null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gzout = new GZIPOutputStream(bos);
            gzout.write(bytes, 0, bytes.length);
            gzout.close();
            byte[] buffer = bos.toByteArray();
            int len = buffer.length;
            out.writeInt(len);
            out.write(buffer, 0, len);
            return bytes.length != 0 ? 100 * buffer.length / bytes.length : 0;
        } else {
            out.writeInt(-1);
            return -1;
        }
    }

    public static String readCompressedString(DataInput in) throws IOException {
        byte[] bytes = readCompressedByteArray(in);
        return bytes == null ? null : new String(bytes, "UTF-8");
    }

    public static int writeCompressedString(DataOutput out, String s) throws IOException {
        return writeCompressedByteArray(out, s != null ? s.getBytes("UTF-8") : null);
    }

    public static void writeString(DataOutput out, String s) throws IOException {
        if (s != null) {
            byte[] buffer = s.getBytes("UTF-8");
            int len = buffer.length;
            out.writeInt(len);
            out.write(buffer, 0, len);
        } else {
            out.writeInt(-1);
        }
    }

    public static String readString(DataInput in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        } else {
            byte[] buffer = new byte[length];
            in.readFully(buffer);
            return new String(buffer, "UTF-8");
        }
    }

    public static void writeStringArray(DataOutput out, String[] s) throws IOException {
        out.writeInt(s.length);

        for (int i = 0; i < s.length; ++i) {
            writeString(out, s[i]);
        }
    }

    public static void writeCompressedStringArray(DataOutput out, String[] s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(s.length);

            for (int i = 0; i < s.length; ++i) {
                writeCompressedString(out, s[i]);
            }
        }
    }

    public static String[] readStringArray(DataInput in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        } else {
            String[] s = new String[len];

            for (int i = 0; i < len; ++i) {
                s[i] = readString(in);
            }

            return s;
        }
    }

    public static String[] readCompressedStringArray(DataInput in) throws IOException {
        int len = in.readInt();
        if (len == -1) {
            return null;
        } else {
            String[] s = new String[len];

            for (int i = 0; i < len; ++i) {
                s[i] = readCompressedString(in);
            }

            return s;
        }
    }

    public static void displayByteArray(byte[] record) {
        int i;
        for (i = 0; i < record.length - 1; ++i) {
            if (i % 16 == 0) {
                System.out.println();
            }

            System.out.print(Integer.toHexString(record[i] >> 4 & 15));
            System.out.print(Integer.toHexString(record[i] & 15));
            System.out.print(",");
        }

        System.out.print(Integer.toHexString(record[i] >> 4 & 15));
        System.out.print(Integer.toHexString(record[i] & 15));
        System.out.println();
    }

    public static <T extends Writable> T clone(T orig, Configuration conf) {
        try {
            T newInst = (T) ReflectionUtils.newInstance(orig.getClass(), conf);
            ReflectionUtils.copy(conf, orig, newInst);
            return newInst;
        } catch (IOException var3) {
            throw new RuntimeException("Error writing/reading clone buffer", var3);
        }
    }

    /** @deprecated */
    @Deprecated
    public static void cloneInto(Writable dst, Writable src) throws IOException {
        ReflectionUtils.cloneWritableInto(dst, src);
    }

    public static void writeVInt(DataOutput stream, int i) throws IOException {
        writeVLong(stream, (long) i);
    }

    public static void writeVLong(DataOutput stream, long i) throws IOException {
        if (i >= -112L && i <= 127L) {
            stream.writeByte((byte) ((int) i));
        } else {
            int len = -112;
            if (i < 0L) {
                i = ~i;
                len = -120;
            }

            for (long tmp = i; tmp != 0L; --len) {
                tmp >>= 8;
            }

            stream.writeByte((byte) len);
            len = len < -120 ? -(len + 120) : -(len + 112);

            for (int idx = len; idx != 0; --idx) {
                int shiftbits = (idx - 1) * 8;
                long mask = 255L << shiftbits;
                stream.writeByte((byte) ((int) ((i & mask) >> shiftbits)));
            }
        }
    }

    public static long readVLong(DataInput stream) throws IOException {
        byte firstByte = stream.readByte();
        int len = decodeVIntSize(firstByte);
        if (len == 1) {
            return (long) firstByte;
        } else {
            long i = 0L;

            for (int idx = 0; idx < len - 1; ++idx) {
                byte b = stream.readByte();
                i <<= 8;
                i |= (long) (b & 255);
            }

            return isNegativeVInt(firstByte) ? ~i : i;
        }
    }

    public static int readVInt(DataInput stream) throws IOException {
        return (int) readVLong(stream);
    }

    public static boolean isNegativeVInt(byte value) {
        return value < -120 || value >= -112 && value < 0;
    }

    public static int decodeVIntSize(byte value) {
        if (value >= -112) {
            return 1;
        } else {
            return value < -120 ? -119 - value : -111 - value;
        }
    }

    public static int getVIntSize(long i) {
        if (i >= -112L && i <= 127L) {
            return 1;
        } else {
            if (i < 0L) {
                i = ~i;
            }

            int dataBits = 64 - Long.numberOfLeadingZeros(i);
            return (dataBits + 7) / 8 + 1;
        }
    }

    public static <T extends Enum<T>> T readEnum(DataInput in, Class<T> enumType)
            throws IOException {
        return Enum.valueOf(enumType, Text.readString(in));
    }

    public static void writeEnum(DataOutput out, Enum<?> enumVal) throws IOException {
        Text.writeString(out, enumVal.name());
    }

    public static void skipFully(DataInput in, int len) throws IOException {
        int total = 0;

        int cur;
        for (boolean var3 = false;
                total < len && (cur = in.skipBytes(len - total)) > 0;
                total += cur) {}

        if (total < len) {
            throw new IOException(
                    "Not able to skip " + len + " bytes, possibly " + "due to end of input.");
        }
    }

    public static byte[] toByteArray(Writable... writables) {
        DataOutputBuffer out = new DataOutputBuffer();

        try {
            Writable[] arr$ = writables;
            int len$ = writables.length;

            for (int i$ = 0; i$ < len$; ++i$) {
                Writable w = arr$[i$];
                w.write(out);
            }

            out.close();
            return out.getData();
        } catch (IOException var6) {
            throw new RuntimeException("Fail to convert writables to a byte array", var6);
        }
    }
}
