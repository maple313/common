package com.qin.common.elasticsearch.mapping;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;

/**
 * @author: WangZB
 * @date: 2019/7/30 21:25
 * @description:
 * @version: 1.0
 */
public class IndexMapping {
    public static void main(String[] args) throws Exception {
        //获取客户端
        RestHighLevelClient client = getRestHighLevelClient();

        client.close();
    }



    private static RestHighLevelClient getRestHighLevelClient() throws Exception {
        //1.指定es的集群  my-application：集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //2.创建es的客户端  注意：9300为tcp端口；9200是http端口
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        return client;
    }
}
