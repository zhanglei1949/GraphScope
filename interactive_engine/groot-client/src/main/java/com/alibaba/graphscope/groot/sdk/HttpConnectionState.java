package com.alibaba.graphscope.groot.sdk;
import com.alibaba.graphscope.groot.sdk.HttpConfig;
import java.util.concurrent.CompletableFuture;

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

public class HttpConnectionState {
    final private String uri;
    private static OkHttpClient client = null;
    public HttpConnectionState(HttpConfig config){
        client = new OkHttpClient.Builder()
                .dispatcher(new Dispatcher(new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                        60L, TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        Util.threadFactory("OkHttp Dispatcher", false)
                )))
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

    public void syncPostWithoutReply(byte[] parameters) throws IOException {
        RequestBody body = RequestBody.create(parameters);
        Request request = new Request.Builder()
                .url(uri)
                .post(body)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
        }
    }

}

