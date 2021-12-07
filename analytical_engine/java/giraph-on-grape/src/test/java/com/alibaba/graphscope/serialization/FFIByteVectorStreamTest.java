package com.alibaba.graphscope.serialization;

import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FFIByteVectorStreamTest {

    static {
        System.loadLibrary("giraph-jni");
    }

    private FFIByteVectorOutputStream outputStream;
    private FFIByteVectorInputStream inputStream;
    private long SIZE = 100;

    @Before
    public void prepare() {
        outputStream = new FFIByteVectorOutputStream();
        outputStream.resize(SIZE);
        inputStream = new FFIByteVectorInputStream();
    }

    @Test
    public void testInt() throws IOException {
        outputStream.reset();
        for (int i = 0; i < 25; ++i) {
            outputStream.writeInt(i);
        }
        FFIByteVector vector = outputStream.getVector();

        inputStream.setVector(vector);
        for (int i = 0; i < 25; ++i) {
            Assert.assertTrue(inputStream.readInt() == i);
        }
    }

    @Test
    public void testStringChar() throws IOException {
        String str = "123456";
        outputStream.reset();
        outputStream.writeChars(str);
        FFIByteVector vector = outputStream.getVector();
        inputStream.setVector(vector);

        char[] chars = new char[str.length()];
        inputStream.read(chars, 0, chars.length);
        String res = new String(chars);
        System.out
            .println("str: " + str.length() + ", " + str + " res: " + res.length() + ", " + res);
        Assert.assertTrue(str.equals(res));
    }
    @Test
    public void testStringUTF() throws IOException {
        String str = "123456";
        outputStream.reset();
        outputStream.writeUTF(str);
        FFIByteVector vector = outputStream.getVector();
        inputStream.setVector(vector);

        String res = inputStream.readUTF();
        System.out
            .println("str: " + str.length() + ", " + str + " res: " + res.length() + ", " + res);
        //Make sure we compare with same encoding
        Assert.assertTrue(str.equals(res));
    }
}
