package com.dsj.esdemo.util;

import lombok.extern.log4j.Log4j2;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Log4j2
@Component
public class EsSearchUtil {

    @Autowired
    private RestHighLevelClient client;

    /**
     * 判断文档的title是否存在（暂定：同一个index下title是唯一的）
     *
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
     * 模糊查询，查询指定index中部分字段和搜索信息匹配的文档
     * 分页查询，按照score降序排序
     *
     * @param searchInfo 用户搜索信息
     * @param page       从第几条开始算起
     * @param pageSize   截取的条数
     * @return
     */
    public SearchHits fuzzySearch(String[] indexes, String[] fields, String searchInfo, Integer page, Integer pageSize) {
        SearchRequest searchRequest = new SearchRequest(indexes);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(searchInfo, fields);
        sourceBuilder.query(queryBuilder);
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        sourceBuilder.from(page);
        sourceBuilder.size(pageSize);
        searchRequest.source(sourceBuilder);

        SearchHits hits = null;
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            hits = searchResponse.getHits();
        } catch (ElasticsearchException e) {
            log.warn("fuzzySearch发生异常! index:{} \n status:{} \n detail:{}", indexes, e.status().getStatus(), e.getDetailedMessage());
        } catch (IOException e) {
            log.warn("fuzzySearch发生异常! index:{} \n {}", indexes, e.getMessage());
        }

        return hits;
    }

    /**
     * 全量查询，查询所有收集的文档
     * 分页查询，（按照索引名称降序排序）
     *
     * @param page     从第几条开始算起
     * @param pageSize 截取的条数
     * @return
     */
    public SearchHits fullSearch(Integer page, Integer pageSize) {
        List<String> indexes = new ArrayList<>();
        SearchHits hits = null;
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesResponse getAliasesResponse = client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetadata>> map = getAliasesResponse.getAliases();
            Set<String> indices = map.keySet();
            for (String index : indices) {
                if (!index.startsWith(".")) {
                    indexes.add(index);
                }
            }

            SearchRequest searchRequest = new SearchRequest(indexes.toArray(new String[indexes.size()]));
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            sourceBuilder.query(queryBuilder);
            sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
            sourceBuilder.from(page);
            sourceBuilder.size(pageSize);
            searchRequest.source(sourceBuilder);

            try {
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                hits = searchResponse.getHits();
            } catch (ElasticsearchException e) {
                log.warn("fullSearch发生异常! \n status:{} \n detail:{}", e.status().getStatus(), e.getDetailedMessage());
            } catch (IOException e) {
                log.warn("fullSearch发生异常! \n {}", e.getMessage());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return hits;
    }
}
