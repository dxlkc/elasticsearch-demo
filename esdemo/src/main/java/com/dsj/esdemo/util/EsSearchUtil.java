package com.dsj.esdemo.util;

import com.dsj.esdemo.model.Document;
import com.dsj.esdemo.model.Knowledge;
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
     * 模糊查询，查询指定index中title、content和搜索信息匹配的文档
     * index不定长
     * 分页查询，按照score降序排序
     *
     * @param searchInfo 用户搜索信息
     * @param page       需要返回的当前页数（从0开始）
     * @param pageSize   当前页内容数量
     * @return
     */
    public List<Document> fuzzySearchByTitleAndContent(String searchInfo, Integer page, Integer pageSize, String... index) {
        List<Document> resultList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery(searchInfo, "title", "content");
        sourceBuilder.query(queryBuilder);
        sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        // 分页
        sourceBuilder.from(page);
        sourceBuilder.size(pageSize);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                Map<String, Object> map = hit.getSourceAsMap();
                Knowledge knowledge = Knowledge.builder()
                        .title(map.get("title").toString())
                        .content(map.get("content").toString())
                        .build();
                Document document = Document.builder()
                        .id(hit.getId())
                        .document(knowledge)
                        .build();
                resultList.add(document);
            }
        } catch (ElasticsearchException e) {
            log.warn("fuzzySearchByTitleAndContent发生异常! index:{} \n status:{} \n detail:{}", index, e.status().getStatus(), e.getDetailedMessage());
        } catch (IOException e) {
            log.warn("fuzzySearchByTitleAndContent发生异常! index:{} \n {}", index, e.getMessage());
        }

        return resultList;
    }

    /**
     * 全量查询，查询所有收集的文档
     * 分页查询，（按照索引名称降序排序）
     *
     * @param page     需要返回的当前页数
     * @param pageSize 当前页内容数量
     * @return
     */
    public List<Document> fullSearch(Integer page, Integer pageSize) {
        List<String> indexes = new ArrayList<>();
        List<Document> resultList = new ArrayList<>();
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesResponse getAliasesResponse =  client.indices().getAlias(request,RequestOptions.DEFAULT);
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
            // 分页
            sourceBuilder.from(page);
            sourceBuilder.size(pageSize);
            searchRequest.source(sourceBuilder);

            try {
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                SearchHits hits = searchResponse.getHits();
                SearchHit[] searchHits = hits.getHits();
                for (SearchHit hit : searchHits) {
                    Map<String, Object> map2 = hit.getSourceAsMap();
                    Knowledge knowledge = Knowledge.builder()
                            .title(map2.get("title").toString())
                            .content(map2.get("content").toString())
                            .build();
                    Document document = Document.builder()
                            .id(hit.getId())
                            .document(knowledge)
                            .build();
                    resultList.add(document);
                }
            } catch (ElasticsearchException e) {
                log.warn("fuzzySearchByTitleAndContent发生异常! \n status:{} \n detail:{}", e.status().getStatus(), e.getDetailedMessage());
            } catch (IOException e) {
                log.warn("fuzzySearchByTitleAndContent发生异常! \n {}", e.getMessage());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultList;
    }
}
