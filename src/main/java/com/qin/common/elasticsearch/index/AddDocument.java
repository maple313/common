package com.qin.common.elasticsearch.index;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: WangZB
 * @date: 2019/7/1 21:53
 * @description:
 * @version: 1.0
 */
public class AddDocument {

    public static void main(String[] args) throws Exception {

        //Client client = getTransportClient();

        //3.查询文档
        /*GetResponse response=client.prepareGet("test_index","test_type","1").execute().actionGet();
        String source = response.getSourceAsString();
        System.out.println(source);*/

        //4.创建文档
        //addIndexDocument(client);

        RestHighLevelClient client = getRestHighLevelClient();

        /*//1.添加文档：jsonString
        addDocumentByJsonString(client);
        //2.添加文档：jsonMap
        //addDocumentByJsonMap(client);
        //3.添加文档：XContentBuilder
        addDocumentByXContentBuilder(client);
        //4.添加文档：Object
        addDocumentByObject(client);*/

        //getRequestData(client);


        boolean exist = isExist(client);
        System.out.println(exist);

        deleteDocument(client);

        //client.close();
    }

    /**
     * @param : client
     * @description : 添加文档————jsonString
     */

    private static void addDocumentByJsonString(RestHighLevelClient client) throws Exception {
        IndexRequest indexRequest = new IndexRequest("my_index", "my_type", "3");
        Map<String, Object> paramsMap = new HashMap<>(16);
        List<String> list = new ArrayList<>();
        paramsMap.put("first_name", "wzb");
        paramsMap.put("last_name", "maple");
        paramsMap.put("age", 20);
        paramsMap.put("about", "道可道，非常道");
        list.add("书法");
        list.add("篮球");
        paramsMap.put("interests", list);
        String jsonString = JSON.toJSONString(paramsMap);
        indexRequest.source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        //index
        String index = indexResponse.getIndex();
        //type
        String type = indexResponse.getType();
        //id
        String id = indexResponse.getId();
        //version:版本
        long version = indexResponse.getVersion();
        //获取结果：创建 | 更新
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            //创建后的操作
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            //更新后的操作
        }
        //shardInfo：分片信息
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        //失败信息
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }
    }

    /**
     * @param : client
     * @description : 添加文档————jsonString
     */

    private static void addDocumentByJsonMap(RestHighLevelClient client) throws Exception {
        IndexRequest indexRequest = new IndexRequest("my_index", "my_type", "4");
        Map<String, Object> paramsMap = new HashMap<>(16);
        List<String> list = new ArrayList<>();
        paramsMap.put("first_name", "斌");
        paramsMap.put("last_name", "maple");
        paramsMap.put("age", 20);
        paramsMap.put("about", "道可道，非常道");
        list.add("书法");
        list.add("篮球");
        paramsMap.put("interests", list);
        indexRequest.source(paramsMap);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        Result result = indexResponse.getResult();
        String s = indexResponse.toString();
        System.out.println(result.toString());
        System.out.println(s);
    }


    /**
     * @param : client
     * @description : 添加文档————XContentBuilder
     */
    private static void addDocumentByXContentBuilder(RestHighLevelClient client) throws IOException {

        //1.创建文档对象
        List<String> list = new ArrayList<>();
        list.add("read");
        list.add("thinking");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("first_name", "欣儿");
        builder.field("last_name", "xin");
        builder.field("age", 18);
        builder.field("about", "beautiful");
        builder.field("interests", list);
        builder.endObject();

        IndexRequest indexRequest = new IndexRequest("my_index", "my_type", "5");
        indexRequest.source(builder);
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        Result result = indexResponse.getResult();
        String s = indexResponse.toString();
        System.out.println(result.toString());
        System.out.println(s);
    }

    /**
     * @param : client
     * @description : 添加文档————Object
     */
    private static void addDocumentByObject(RestHighLevelClient client) throws IOException {

        List<String> list = new ArrayList<>();
        list.add("read");
        list.add("thinking");

        IndexRequest request = new IndexRequest("my_index", "my_type", "6")
                .source("first_name", "didi", "last_name", "dd", "age", 18, "about", "fengyue", "interests", list);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        Result result = indexResponse.getResult();
        String s = indexResponse.toString();
        System.out.println(result.toString());
        System.out.println(s);
    }

    private static Client getTransportClient() throws Exception {
        //1.指定es的集群  my-application：集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //2.创建es的客户端  注意：9300为tcp端口；9200是http端口
        TransportClient client = new PreBuiltTransportClient(settings).
                addTransportAddress(
                        new TransportAddress(
                                InetAddress.getByName("127.0.0.1"), 9300));
        return client;
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

    private static Map<String, Object> getParamsMap() {
        Map<String, Object> paramsMap = new HashMap<>(16);
        List<String> list = new ArrayList<>();
        paramsMap.put("first_name", "欣儿");
        paramsMap.put("last_name", "xin");
        paramsMap.put("age", 18);
        paramsMap.put("about", "beautiful");
        list.add("eat");
        list.add("sleep");
        paramsMap.put("interests", list);
        return paramsMap;
    }

    /**
     * @param : client
     * @description : 获取文档
     */
    private static void getRequestData(RestHighLevelClient client) throws IOException {
        GetRequest request = new GetRequest("my_index", "my_type", "1");
        /*//includes:包含字段
        String[] includes = new String[]{"first_name", "first_name",};
        //excludes:包含字段
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);*/

        //监听器
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                System.out.println("获取成功");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("获取失败");
            }
        };

        //异步获取
        client.getAsync(request, RequestOptions.DEFAULT, listener);


        //异步获取
        GetResponse response = client.get(request, RequestOptions.DEFAULT);

        Map<String, Object> source = response.getSource();
        String sourceAsString = response.getSourceAsString();
        Map<String, Object> sourceAsMap = response.getSourceAsMap();
        byte[] sourceAsBytes = response.getSourceAsBytes();
        BytesReference sourceInternal = response.getSourceInternal();


        System.out.println(request);
        System.out.println(response);

        System.out.println(source);
        System.out.println(sourceAsString);
        System.out.println(sourceAsMap);
        System.out.println(sourceAsBytes);
        System.out.println(sourceInternal);


    }

    /**
     * @param : client
     * @description : 判断文档是否存在
     */
    private static boolean isExist(RestHighLevelClient client) throws IOException{

        GetRequest request = new GetRequest("my_index", "my_type", "6");
        //1.同步判断
        boolean exists = client.exists(request, RequestOptions.DEFAULT);

        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean exists) {
               if (exists){
                   System.out.println("文档存在");
               }else {
                   System.out.println("文档不存在");
               }
            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        //2.异步判断
        //client.existsAsync(request, RequestOptions.DEFAULT, listener);

        return exists;
    }


    /**
     * @param : client
     * @description : 删除文档
     */
    private static void deleteDocument(RestHighLevelClient client) throws IOException{

        DeleteRequest request = new DeleteRequest("my_index", "my_type", "6");

        //设置请求超时时间：2分钟
        request.timeout(TimeValue.timeValueMinutes(2));
        //request.timeout("2m");

        //同步删除
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        //异步删除
        ActionListener<DeleteResponse> listener = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                System.out.println("删除后操作");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("删除失败");
            }
        };
        //client.deleteAsync(request, RequestOptions.DEFAULT, listener);

        //返回信息
        System.out.println(deleteResponse.toString());
        String index = deleteResponse.getIndex();
        String type = deleteResponse.getType();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }
    }
}
