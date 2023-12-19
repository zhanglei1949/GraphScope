package com.alibaba.graphscope.groot.sdk;
import com.alibaba.graphscope.groot.sdk.schema.Edge;
import com.alibaba.graphscope.groot.sdk.schema.Vertex;
import com.alibaba.graphscope.groot.sdk.HttpClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class HttpClientTest {
    public static void main(String[] args) throws Exception {
        // 当文本光标位于高亮显示的文本处时按 Opt+Enter，
        // 可查看 IntelliJ IDEA 对于如何修正该问题的建议。

        HttpClient.HttpClientBuilder c = HttpClient.newBuilder();
        Map<String, String> mp = new HashMap<>();
        mp.put("url", "http://172.16.0.141:10000");

        c.onInit(mp);
        HttpClient client = c.build();
           List<Vertex> vertices = new ArrayList<Vertex>();
        {
            Map<String, String> properties = new HashMap<String, String>();
            properties.put("id", "center_id_1");
            Vertex v = new Vertex("center", properties);
            vertices.add(v);
            Map<String, String> properties2 = new HashMap<String, String>();
            properties2.put("id", "center_id_2");
            Vertex v2 = new Vertex("center", properties2);
            vertices.add(v2);

        }
        {

            Map<String, String> properties = new HashMap<String, String>();
            properties.put("id","medium_id_1");
            properties.put("type", "type_1");
            properties.put("weight","0.5");
            Vertex v = new Vertex("medium",properties);
            vertices.add(v);

        }
        {

            Map<String, String> properties = new HashMap<String, String>();
            properties.put("id","medium_id_2");
            properties.put("type", "type_2");
            properties.put("weight","0.76");
            Vertex v = new Vertex("medium",properties);
            vertices.add(v);
        }
        List<Edge> edges = new ArrayList<Edge>();

        {
            Map<String,String> src_pk = new HashMap<String,String>();
            src_pk.put("id","center_id_1");
            Map<String,String> dst_pk = new HashMap<String,String>();
            dst_pk.put("id","medium_id_1");
            Map<String,String> edge_prop = new HashMap<String,String>();
            edge_prop.put("weight","0.98");
            Edge e = new Edge("connect","center","medium",src_pk,dst_pk,edge_prop);
            edges.add(e);
        }

        {
            Map<String,String> src_pk = new HashMap<String,String>();
            src_pk.put("id","center_id_1");
            Map<String,String> dst_pk = new HashMap<String,String>();
            dst_pk.put("id","medium_id_2");
            Map<String,String> edge_prop = new HashMap<String,String>();
            edge_prop.put("weight","0.99");
            Edge e = new Edge("connect","center","medium",src_pk,dst_pk,edge_prop);
            edges.add(e);
        }


        client.updateVerticesAndEdges(vertices,edges);
        int x = 114;
        String s = String.valueOf(x);
        System.out.println(s);
    }
}
