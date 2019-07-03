package com.qin.common.util;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author: WangZB
 * @date: 2019/7/1 21:53
 * @description:
 * @version: 1.0
 */
public class ElasticsearchUtil {

    public static void main(String[] args) throws UnknownHostException {

        //1.指定es的集群  my-application：集群名称
        Settings settings=Settings.builder().put("cluster.name","my-application").build();

        //2.创建es的客户端  注意：9300为tcp端口；9200是http端口
        TransportClient client=new PreBuiltTransportClient(settings).
                addTransportAddress(
                new TransportAddress(
                        InetAddress.getByName("127.0.0.1"),9300));

        //3.查询文档
        GetResponse response=client.prepareGet("test_index","test_type","1").execute().actionGet();
        String source = response.getSourceAsString();
        System.out.println(source);
        client.close();
    }
}
