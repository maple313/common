package com.qin.common.elasticsearch.query;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: WangZB
 * @date: 2019/7/15 20:05
 * @description:
 * @version: 1.0
 */
public class QueryDocument {
    public static void main(String[] args) throws Exception {
        //获取客户端
        RestHighLevelClient client = getRestHighLevelClient();

        //searchDocumentByMatchAll(client);
        //searchDocumentByTerm(client);
        //searchDocumentByDsl(client);

        //searchDocumentByHighLight(client);

        client.close();


    }


    /**
     * @param : client
     * @description : 查询文档————matchAll
     */
    private static void searchDocumentByMatchAll(RestHighLevelClient client) throws Exception {

        //可以指定index,也可以不指定，不指定查所有
        SearchRequest searchRequest = new SearchRequest("my_index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        //可以指定type,也可以不指定，不指定查所有
        searchRequest.types("my_type");

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //查询信息
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        //匹配查询的结果集
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit

            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        }
    }

    /**
     * @param : client
     * @description : 查询文档————matchQueryBuilder————term
     */
    private static void searchDocumentByTerm(RestHighLevelClient client) throws Exception {

        SearchRequest searchRequest = new SearchRequest();
        //可以指定多个index,也可以不指定，不指定查所有
        searchRequest.indices("my_index");
        //可以指定多个type,也可以不指定，不指定查所有
        searchRequest.types("my_type");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //term查询
        searchSourceBuilder.query(QueryBuilders.termQuery("first_name","斌"));
        //分页:第一页；查询两条
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //查询信息
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        //匹配查询的结果集
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit

            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        }
    }

    /**
     * @param : client
     * @description : 查询文档————matchQueryBuilder————DSL
     */
    private static void searchDocumentByDsl(RestHighLevelClient client) throws Exception {

        SearchRequest searchRequest = new SearchRequest();
        //可以指定多个index,也可以不指定，不指定查所有
        searchRequest.indices("my_index");
        //可以指定多个type,也可以不指定，不指定查所有
        searchRequest.types("my_type");


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        //指定排序：score
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder ("first_name","王志斌");
        //在匹配查询上启用模糊匹配
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        //在匹配查询上设置前缀长度选项
        matchQueryBuilder.prefixLength(3);
        //设置最大扩展选项以控制查询的模糊过程
        matchQueryBuilder.maxExpansions(10);

        searchSourceBuilder.query(matchQueryBuilder);
        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //查询信息
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        //匹配查询的结果集
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit

            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        }
    }

    /**
     * @param : client
     * @description : 查询文档————HighLight(高亮显示)
     */
    private static void searchDocumentByHighLight(RestHighLevelClient client) throws Exception {

        SearchRequest searchRequest = new SearchRequest();
        //可以指定多个index,也可以不指定，不指定查所有
        searchRequest.indices("my_index");
        //可以指定多个type,也可以不指定，不指定查所有
        searchRequest.types("my_type");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //term查询
        searchSourceBuilder.query(QueryBuilders.termQuery("first_name","斌"));
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(2);
        //指定排序：score
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));

        HighlightBuilder highlightBuilder = new HighlightBuilder ();
        //高亮字段
        HighlightBuilder.Field firstNameField =new HighlightBuilder.Field("first_name");
        //高亮类型：unified, plain和fvh
        firstNameField.highlighterType("unified");
        //高亮前缀
        highlightBuilder.preTags("<em>");
        //高亮后缀
        highlightBuilder.postTags("</em>");

        highlightBuilder.field(firstNameField);

        searchSourceBuilder.highlighter(highlightBuilder);
        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //查询信息
        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        //匹配查询的结果集
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit

            String index = hit.getIndex();
            String type = hit.getType();
            String id = hit.getId();
            float score = hit.getScore();

            //高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields.toString());


            String sourceAsString = hit.getSourceAsString();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsString);
            System.out.println(sourceAsMap);
        }
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
