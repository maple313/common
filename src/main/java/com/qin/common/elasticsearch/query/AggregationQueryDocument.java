package com.qin.common.elasticsearch.query;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * @author: WangZB
 * @date: 2019/7/24 21:43
 * @description:
 * @version: 1.0
 */
public class AggregationQueryDocument {

    public static void main(String[] args) throws Exception {
        //获取客户端
        RestHighLevelClient client = getRestHighLevelClient();
        searchDocumentByAggregations(client);
        client.close();
    }

    /**
     * @param client
     * @description 查询文档————Aggregations(聚合)
     */
    private static void searchDocumentByAggregations(RestHighLevelClient client) throws Exception {

        //查询请求
        SearchRequest searchRequest = new SearchRequest();
        //可以指定多个index,也可以不指定，不指定查所有
        searchRequest.indices("agg_index");
        //可以指定多个type,也可以不指定，不指定查所有
        searchRequest.types("agg_type");
        //构建查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //1.按group分组
        TermsAggregationBuilder testAgg = AggregationBuilders.terms("group_grade").field("grade");
        //2.分组后求平均年龄:testAgg的子聚合
        testAgg.subAggregation(AggregationBuilders.avg("avg_age").field("age"));
        //3.分组后文档数量计数
        /*AggregationBuilders.min("min_id").field("age"); //最小年龄
        AggregationBuilders.max("max_id").field("age"); //最大年龄*/
        testAgg.subAggregation(AggregationBuilders.count("count_id").field("_id"));
        //查询所有
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        //构建聚合
        searchSourceBuilder.aggregation(testAgg);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        //查询信息
        Aggregations aggregations = searchResponse.getAggregations();
        Terms terms = searchResponse.getAggregations().get("group_grade");

        for (Terms.Bucket k:terms.getBuckets()){
            Object key = k.getKey();
            long docCount = k.getDocCount();

            //子聚合信息
            Avg avg = k.getAggregations().get("avg_age");
            ValueCount count= k.getAggregations().get("count_id");
            long value1 = count.getValue();
            double value = avg.getValue();


            System.out.println("group:"+key+";"+"stu:"+value1+"count:"+docCount+";"+"avg_age:"+value);
        }


        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        float maxScore = hits.getMaxScore();

        //匹配查询的结果集
        SearchHit[] searchHits = hits.getHits();

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
