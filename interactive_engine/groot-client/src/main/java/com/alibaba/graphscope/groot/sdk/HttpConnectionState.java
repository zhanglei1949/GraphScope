package com.alibaba.graphscope.groot.sdk;

import com.alibaba.graphscope.groot.sdk.HttpConfig;

import java.io.IOException;
import java.util.concurrent.*;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import okhttp3.internal.Util;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

class ResponseFuture implements Callback {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    public ResponseFuture() {
    }

    @Override
    public void onFailure(Call call, IOException e) {
        e.printStackTrace();
        future.completeExceptionally(e);
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
        future.complete(response);
    }
}

final class Decoder {
    public Decoder(byte[] bs) {
        this.bs = bs;
        this.loc = 0;
        this.len = this.bs.length;
    }

    public static int get_int(byte[] bs, int loc) {
        int ret = (bs[loc + 3] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 2] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 1] & 0xff);
        ret <<= 8;
        ret |= (bs[loc] & 0xff);
        return ret;
    }

    public static long get_long(byte[] bs, int loc) {
        long ret = (bs[loc + 7] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 6] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 5] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 4] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 3] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 2] & 0xff);
        ret <<= 8;
        ret |= (bs[loc + 1] & 0xff);
        ret <<= 8;
        ret |= (bs[loc] & 0xff);
        return ret;
    }

    public long get_long() {
        long ret = get_long(this.bs, this.loc);
        this.loc += 8;
        return ret;
    }

    public int get_int() {
        int ret = get_int(this.bs, this.loc);
        this.loc += 4;
        return ret;
    }

    public byte get_byte() {
        return (byte) (bs[loc++] & 0xFF);
    }

    public String get_string() {
        int strlen = this.get_int();
        String ret = new String(this.bs, this.loc, strlen);
        this.loc += strlen;
        return ret;
    }

    public boolean empty() {
        return loc == len;
    }

    byte[] bs;
    int loc;
    int len;
}

public class HttpConnectionState {
    final private String uri;
    private static OkHttpClient client = null;

    public HttpConnectionState(HttpConfig config) {
        client = new OkHttpClient.Builder()
                .dispatcher(new Dispatcher(new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                        60L, TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        Util.threadFactory("OkHttp Dispatcher", false))))
                .connectionPool(new ConnectionPool(config.getConnectPoolMaxIdle(),
                        config.getKeepAliveDuration(),
                        TimeUnit.MILLISECONDS))
                .readTimeout(config.getReadTimeout(), TimeUnit.MILLISECONDS)
                .connectTimeout(config.getConnectTimeout(), TimeUnit.MILLISECONDS)
                .build();
        client.dispatcher().setMaxRequests(config.getMaxRequests());
        client.dispatcher().setMaxRequestsPerHost(config.getMaxRequestsPerHost());
        uri = config.getServerAddr() + "/interactive/update";
    }

    public long syncPost(byte[] parameters) throws IOException {
        RequestBody body = RequestBody.create(parameters);
        Request request = new Request.Builder()
                .url(uri)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            byte[] bs = response.body().bytes();
            Decoder decoder = new Decoder(bs);
            return decoder.get_long();
        }
    }

}
