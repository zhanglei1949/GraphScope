package com.alibaba.graphscope.serialization;

import com.alibaba.graphscope.stdcxx.FFIByteVector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FFIByteVectorOutputStreamTest {

    static {
        System.loadLibrary("giraph-jni");
    }
    private FFIByteVectorOutputStream stream;

    @Before
    public void prepare(){
    }

    @Test
    public void testInt() throws IOException {
        stream = new FFIByteVectorOutputStream();
        stream.reserve(100);
	stream.resize(100);
        for (int i = 0; i < 25; ++i){
            stream.writeInt(i);
        }
        FFIByteVector vector = stream.getVector();
        for (int i = 0; i < 25; ++i){
//            System.out.println(i + " " + vector.getRawInt(i * 4));
            Assert.assertTrue(vector.getRawInt(i * 4)== i);
        }
    }

    @Test
    public void testString() throws IOException {
        stream = new FFIByteVectorOutputStream();
        stream.reserve(100);
	stream.resize(100);
        String str = "123456";
//        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        stream.writeChars(str);
        FFIByteVector vector = stream.getVector();
        char[] chars = new char[str.length()];
        for (int i = 0; i < chars.length; ++i){
//            Assert.assertTrue(vector.getRawInt(i * 4)== i);
            chars[i] = vector.getRawChar(i * 2);
        }

        String res = new String(chars);
        System.out.println("str: "+str.length()+ ", " + str + " res: "+ res.length() + ", "  + res);
	//Make sure we compare with same encoding
        Assert.assertTrue(str.equals(res));
    }
}
