package com.alibaba.graphscope.groot.sdk;

import com.alibaba.graphscope.groot.sdk.api.Writer;
import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;

import java.io.IOException;
import java.util.Map;
import java.util.List;

final class Encoder {
    public Encoder(byte[] bs) {
        this.bs = bs;
        this.loc = 0;
    }

    public static int serialize_long(byte[] bytes, int offset, long value) {
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        return offset;
    }

    public static int serialize_int(byte[] bytes, int offset, int value) {
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        value >>= 8;
        bytes[offset++] = (byte) (value & 0xFF);
        return offset;
    }

    public static int serialize_byte(byte[] bytes, int offset, byte value) {
        bytes[offset++] = value;
        return offset;
    }

    public static int serialize_bytes(byte[] bytes, int offset, byte[] value) {
        offset = serialize_int(bytes, offset, value.length);
        System.arraycopy(value, 0, bytes, offset, value.length);
        return offset + value.length;
    }

    private void put_properties(Map<String,String> mp){
        // 4 +
        put_int(mp.size());
        mp.entrySet().stream()
                .forEach(entry ->{
                    put_bytes(entry.getKey().getBytes());
                    put_bytes(entry.getValue().getBytes());
                });
    }

    public void put_edge(Edge e){
        put_bytes(e.srcLabel.getBytes());
        put_bytes(e.dstLabel.getBytes());
        put_bytes(e.label.getBytes());
        put_properties(e.srcPk);
        put_properties(e.dstPk);
        put_properties(e.properties);
    }
    public void put_edges(List<Edge> edges){
        put_int(edges.size());
        edges.stream().forEach(e ->
                put_edge(e)
        );
    }

    public void put_vertex(Vertex v){
        put_bytes(v.label.getBytes());
        put_properties(v.getProperties());
    }
    public void put_vertices(List<Vertex> vertices){
        put_int(vertices.size());
        vertices.stream().forEach(v ->
                put_vertex(v)
        );
    }

    public void put_int(int value) {
        this.loc = serialize_int(this.bs, this.loc, value);
    }

    public void put_byte(byte value) {
        this.loc = serialize_byte(this.bs, this.loc, value);
    }

    public void put_double(double value){
        long val = Double.doubleToRawLongBits(value);
        put_long(val);
    }
    public void put_long(long value) {
        this.loc = serialize_long(this.bs, this.loc, value);
    }

    public void put_bytes(byte[] bytes) {
        this.loc = serialize_bytes(this.bs, this.loc, bytes);
    }

    byte[] bs;
    int loc;
}
public class HttpClient implements Writer {

    public long executeOperation(byte[] query)  {
        try
        {
            return connectionState.syncPost(query);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Block until this snapshot becomes available.
     * @param snapshotId the snapshot id to be flushed
     */
    public boolean remoteFlush(long snapshotId) {
        int len = 8 + 1;
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_long(snapshotId);
        input.put_byte((byte)0);
        long cur = executeOperation(bs);
        return (cur <= snapshotId);
    }

    @Override
    public long addVertex(Vertex vertex) {
        int len = getVertexLength(vertex);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertex(vertex);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long addVertices(List<Vertex> vertices) {
        int len = getVertiesLength(vertices);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long updateVertex(Vertex vertex) {
        int len = getVertexLength(vertex);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertex(vertex);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long updateVertices(List<Vertex> vertices) {
        int len = getVertiesLength(vertices);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long clearVertexProperty(Vertex vertex) {
        int len = getVertexLength(vertex);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertex(vertex);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long clearVertexProperties(List<Vertex> vertices) {
        int len = getVertiesLength(vertices);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long deleteVertex(Vertex vertex) {
        int len = getVertexLength(vertex);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertex(vertex);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long deleteVertices(List<Vertex> vertices) {
        int len = getVertiesLength(vertices);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long addEdge(Edge edge) {
        int len = getEdgeLength(edge);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edge(edge);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long addEdges(List<Edge> edges) {
        int len = getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edges(edges);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long updateEdge(Edge edge) {
        int len = getEdgeLength(edge);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edge(edge);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long updateEdges(List<Edge> edges) {
        int len = getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edges(edges);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long clearEdgeProperty(Edge edge) {
        int len = getEdgeLength(edge);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edge(edge);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long clearEdgeProperties(List<Edge> edges) {
        int len = getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edges(edges);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long deleteEdge(Edge edge) {
        int len = getEdgeLength(edge);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edge(edge);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long deleteEdges(List<Edge> edges) {
        int len = getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_edges(edges);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    @Override
    public long addVerticesAndEdges(List<Vertex> vertices, List<Edge> edges) {
        int len = getVertiesLength(vertices) + getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        input.put_edges(edges);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    private int getPropertiesLength(Map<String,String> mp){
        return mp.entrySet().stream()
                .mapToInt(entry -> entry.getKey().length() + entry.getValue().length())
                .sum() + 4 + mp.size() * 8;
    }

    private int getVertexLength(Vertex v){
        return v.label.length() + 4 + getPropertiesLength(v.properties);
    }

    private int getVertiesLength(List<Vertex> vertices){
        return 4 + vertices.stream().mapToInt(v ->
                v.label.length() + 4 + getPropertiesLength(v.properties)).sum();
    }

    private int getEdgeLength(Edge e){
        return 12 + e.srcLabel.length()
                + e.dstLabel.length() + e.label.length()
                + getPropertiesLength(e.srcPk) + getPropertiesLength(e.dstPk)
                + getPropertiesLength(e.properties);
    }
    private int getEdgesLength(List<Edge> edges){
        return 4 + edges.stream().mapToInt(e -> 12 + e.srcLabel.length()
                + e.dstLabel.length() + e.label.length()
                + getPropertiesLength(e.srcPk) + getPropertiesLength(e.dstPk)
                + getPropertiesLength(e.properties)).sum();
    }

    @Override
    public long updateVerticesAndEdges(List<Vertex> vertices, List<Edge> edges) {
        int len = getVertiesLength(vertices) + getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        input.put_edges(edges);
        input.put_byte((byte)(3));
        return executeOperation(bs);
    }

    @Override
    public long deleteVerticesAndEdges(List<Vertex> vertices, List<Edge> edges) {
        int len = getVertiesLength(vertices) + getEdgesLength(edges);
        byte[] bs = new byte[len + 1];
        Encoder input = new Encoder(bs);
        input.put_vertices(vertices);
        input.put_edges(edges);
        // input.put_byte((byte)(3));
        // executeOperation(bs);
        return 0;
    }

    private HttpClient(HttpConfig config){
        connectionState = new HttpConnectionState(config);
    }

    public static HttpClient.HttpClientBuilder newBuilder() {
        return new HttpClient.HttpClientBuilder();
    }

    public static class HttpClientBuilder {
        private HttpConfig config;
        public void onInit(Map<String,String> map) throws Exception {
            config = new HttpConfig();
            if(null == map.get("url")){
                throw new Exception("No URL provided");
            }
            config.setServerAddr(map.get("url"));
            if(null != map.get("readTimeout")){
                config.setReadTimeout(Integer.valueOf(map.get("readTimeout")));
            }
            if(null != map.get("connectTimeout")){
                config.setConnectTimeout(Integer.valueOf(map.get("connectTimeout")));
            }
            if(null != map.get("connectPoolMaxIdle")){
                config.setConnectPoolMaxIdle(Integer.valueOf(map.get("connectPoolMaxIdle")));
            }
            if(null != map.get("keepAliveDuration")){
                config.setKeepAliveDuration(Integer.valueOf(map.get("keepAliveDuration")));
            }
            if(null != map.get("maxRequestsPerHost")){
                config.setMaxRequestsPerHost(Integer.valueOf(map.get("maxRequestsPerHost")));
            }
            if(null != map.get("maxRequests")){
                config.setMaxRequests(Integer.valueOf(map.get("maxRequests")));
            }
        }
        private HttpClientBuilder() {}


        public HttpClient build() {
            return new HttpClient(config);
        }
    }
    private HttpConnectionState connectionState;
}
