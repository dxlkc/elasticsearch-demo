package com.dsj.esdemo.util;

import lombok.extern.log4j.Log4j2;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
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
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("title", title);
        sourceBuilder.query(matchQueryBuilder);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            // 为了精确匹配
            if (searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                    if (hit.getScore() >= 1 && sourceAsMap.get("title").equals(title)){
                        result = true;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            log.warn("isDocumentTitleExist发生异常! index:{}, title:{} \n {}", index, title, e.getMessage());
        } catch (ElasticsearchException elasticException) {
            log.info("index不存在！可以新增！");
        }

        return result;
    }
}
