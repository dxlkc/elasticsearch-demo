package com.dsj.esdemo.service.Impl;

import com.dsj.esdemo.model.Document;
import com.dsj.esdemo.model.Knowledge;
import com.dsj.esdemo.service.SearchService;
import com.dsj.esdemo.util.EsSearchUtil;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SearchServiceImpl implements SearchService {

    @Autowired
    private EsSearchUtil searchUtil;

    @Override
    public List<Document> fuzzySearch(String[] index, String[] fields, String searchInfo, Integer page, Integer pageSize) {
        List<Document> resultList = new ArrayList<>();
        SearchHits hits = searchUtil.fuzzySearch(index, fields, searchInfo, page, pageSize);
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
        return resultList;
    }

    @Override
    public List<Document> fullSearch(Integer page, Integer pageSize) {
        List<Document> resultList = new ArrayList<>();
        SearchHits hits = searchUtil.fullSearch(page, pageSize);
        for (SearchHit hit : hits.getHits()) {
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
        return resultList;
    }
}
