package com.alibaba.graphscope.serialization;

import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FFIByteVectorOutputStreamTest {
    private FFIByteVectorOutputStream stream;

    @Before
    public void prepare(){
        stream = new FFIByteVectorOutputStream();
        stream.reserve(100);
    }

    @Test
    private void testInt() throws IOException {
        for (int i = 0; i < 25; ++i){
            stream.writeInt(i);
        }
        FFIByteVector vector = stream.getVector();
        for (int i = 0; i < 25; ++i){
            Assert.assertTrue(vector.getRawInt(i * 4)== i);
        }
    }

    @Test
    private void testString() throws IOException {
        String str = "123456";
//        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        stream.writeUTF(str);
        FFIByteVector vector = stream.getVector();
        byte[] bytes = new byte[str.length() * 3 + 4];
        for (int i = 0; i < bytes.length; ++i){
//            Assert.assertTrue(vector.getRawInt(i * 4)== i);
            bytes[i] = vector.getRaw(i);
        }

        String res = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("str: " + str + " res: " + res);
        Assert.assertTrue(str.equals(res));
    }
}
