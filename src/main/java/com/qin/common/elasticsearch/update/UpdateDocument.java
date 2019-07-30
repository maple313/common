package com.qin.common.elasticsearch.update;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: WangZB
 * @date: 2019/7/13 19:54
 * @description:
 * @version: 1.0
 */
public class UpdateDocument {
    public static void main(String[] args) throws Exception {

        //获取客户端
        RestHighLevelClient client = getRestHighLevelClient();

        //updateByJsonString(client);
        //updateByJsonMap(client);
        //updateByXContentBuilder(client);
        //updateByObject(client);

        upsertByJsonString(client);

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

    /**
     * @param : client
     * @description : 修改文档————jsonString
     */
    private static void updateByJsonString(RestHighLevelClient client) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("my_index", "my_type", "5");

        Map<String, Object> paramsMap = new HashMap<>(16);
        List<String> list = new ArrayList<>();
        paramsMap.put("first_name", "冰河世纪");
        String jsonString = JSON.toJSONString(paramsMap);

        updateRequest.fetchSource(true);

        updateRequest.doc(jsonString, XContentType.JSON);

        //同步更新
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        //异步更新
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        //client.updateAsync(updateRequest, RequestOptions.DEFAULT, listener);


        //返回信息
        GetResult getResult = updateResponse.getGetResult();
        System.out.println(getResult.sourceAsString());
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }
    }

    /**
     * @param : client
     * @description : 修改文档————jsonMap
     */
    private static void updateByJsonMap(RestHighLevelClient client) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("my_index", "my_type", "5");

        Map<String, Object> paramsMap = new HashMap<>(16);
        List<String> list = new ArrayList<>();
        paramsMap.put("first_name", "冰河");

        updateRequest.fetchSource(true);

        updateRequest.doc(paramsMap);

        //同步更新
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        //异步更新
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        //client.updateAsync(updateRequest, RequestOptions.DEFAULT, listener);


        //返回信息
        GetResult getResult = updateResponse.getGetResult();
        System.out.println(getResult.sourceAsString());
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }
    }

    /**
     * @param : client
     * @description : 修改文档————XContentBuilder
     */
    private static void updateByXContentBuilder(RestHighLevelClient client) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("my_index", "my_type", "5");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("first_name", "builder");
        builder.endObject();

        updateRequest.fetchSource(true);

        updateRequest.doc(builder);

        //同步更新
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        //异步更新
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        //client.updateAsync(updateRequest, RequestOptions.DEFAULT, listener);


        //返回信息
        GetResult getResult = updateResponse.getGetResult();
        System.out.println(getResult.sourceAsString());
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }
    }

    /**
     * @param : client
     * @description : 修改文档————Object
     */
    private static void updateByObject(RestHighLevelClient client) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("my_index", "my_type", "5");

        updateRequest.fetchSource(true);

        updateRequest.doc("first_name", "object","last_name", "object");

        //同步更新
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        //异步更新
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        //client.updateAsync(updateRequest, RequestOptions.DEFAULT, listener);


        //返回信息
        GetResult getResult = updateResponse.getGetResult();
        System.out.println(getResult.sourceAsString());
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }
    }

    /**
     * @param : client
     * @description : 修改文档————jsonString
     */
    private static void upsertByJsonString(RestHighLevelClient client) throws Exception {
        UpdateRequest updateRequest = new UpdateRequest("my_index", "my_type", "7");

        //如果尚未存在，则表明必须将部分文档用作upsert文档
        updateRequest.docAsUpsert(true);
        //禁用noop检测
        updateRequest.detectNoop(false);
        //更新后是否获取_source
        updateRequest.fetchSource(true);

        Map<String, Object> paramsMap = new HashMap<>(16);
        paramsMap.put("about", "如果document存在就更新，否则新增");
        String jsonString = JSON.toJSONString(paramsMap);



        updateRequest.upsert(jsonString, XContentType.JSON);
        updateRequest.doc(jsonString,XContentType.JSON);

        //同步更新
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);

        //异步更新
        ActionListener<UpdateResponse> listener = new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        //client.updateAsync(updateRequest, RequestOptions.DEFAULT, listener);


        //返回信息
        GetResult getResult = updateResponse.getGetResult();
        System.out.println(getResult.sourceAsString());
        String index = updateResponse.getIndex();
        String type = updateResponse.getType();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }
    }
}
