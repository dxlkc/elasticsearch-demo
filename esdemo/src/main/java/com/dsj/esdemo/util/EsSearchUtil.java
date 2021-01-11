package com.dsj.esdemo.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Log4j2
@Component
public class EsSearchUtil {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 判断文档的title是否存在（暂定：同一个index下title是唯一的）
     * @param index 索引名称
     * @param title 标题名称
     * @return 存在true，不存在false
     */
    public Boolean isDocumentTitleExist(String index, String title) {
        boolean result = false;
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders
                .boolQuery()
                .filter(QueryBuilders.termQuery("title.keyword", title));
        sourceBuilder.query(queryBuilder);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            if (searchHits.length > 0) {
                result = true;
            }
        } catch (IOException e) {
            log.warn("isDocumentTitleExist发生异常! index:{}, title:{} \n {}", index, title, e.getMessage());
        } catch (ElasticsearchException elasticException) {
            log.info("index不存在！可以新增！");
        }

        return result;
    }

    /**
     * 模糊查询，查询所有index中title、content和搜索信息匹配的文档（考虑和下面一个合并）
     * 分页查询，按照score降序排序
     * @param searchInfo 用户搜索信息
     * @param page       需要返回的当前页数
     * @param pageSize   当前页内容数量
     * @return
     */
    public JSONObject fuzzySearchByTitleAndContent(String searchInfo, String page, String pageSize) {

        return null;
    }

    /**
     * 模糊查询，查询指定index中title、content和搜索信息匹配的文档
     * 分页查询，按照score降序排序
     * @param searchInfo 用户搜索信息
     * @param index      指定的索引名称
     * @param page       需要返回的当前页数
     * @param pageSize   当前页内容数量
     * @return
     */
    public JSONObject fuzzySearchByIndexAndTitleAndContent(String searchInfo, String index, String page, String pageSize) {

        return null;
    }

    /**
     * 全量查询，查询所有收集的文档
     * 分页查询，（按照索引名称降序排序）
     * @param page  需要返回的当前页数
     * @param pageSize 当前页内容数量
     * @return
     */
    public JSONObject fullSearch(String page, String pageSize) {

        return null;
    }
}
