package com.dsj.esdemo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dsj.esdemo.model.Document;
import com.dsj.esdemo.model.Knowledge;
import com.dsj.esdemo.util.EsSearchUtil;
import com.dsj.esdemo.util.EsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
public class TestController {

    @Autowired
    public EsUtil esUtil;
    @Autowired
    public EsSearchUtil esSearchUtil;

    @PostMapping("/get")
    public String test(@RequestParam String index, @RequestParam String id) throws Exception {
        JSONObject jsonObject = esUtil.getDocument(index, id);
        return JSONObject.toJSONString(jsonObject);
    }

    @PostMapping("/index/create")
    public boolean createIndex(@RequestParam String index) {
        return esUtil.createIndex(index);
    }

    @PostMapping("/index/delete")
    public boolean deleteIndex(@RequestParam String index) {
        return esUtil.deleteIndex(index);
    }

    @PostMapping("/document/add")
    public boolean addDocument(@RequestParam String index, @RequestParam String title, @RequestParam String content) {
        Knowledge knowledge = Knowledge.builder()
                .title(title)
                .content(content)
                .build();
        String jsonString = JSON.toJSONString(knowledge);
        System.out.println(jsonString);
        return esUtil.addDocument(index, UUID.randomUUID().toString(), title, jsonString);
    }

    @PostMapping("/document/delete")
    public boolean deleteDocument(@RequestParam String index, @RequestParam String id) {
        return esUtil.deleteDocument(index, id);
    }

    @PostMapping("/document/exist")
    public boolean isDocumentExist(@RequestParam String index, @RequestParam String title) {
        return esSearchUtil.isDocumentTitleExist(index, title);
    }

    @PostMapping("/find/all")
    public String fuzzySearchByTitleAndContent(@RequestParam String searchInfo, @RequestParam Integer page,
                                               @RequestParam Integer pageSize, @RequestParam String[] index) {
        List<Document> result = esSearchUtil.fuzzySearchByTitleAndContent(searchInfo, page, pageSize, index);
        return JSON.toJSONString(result);
    }

    @PostMapping("/find/full")
    public String fullSearch(@RequestParam Integer page, @RequestParam Integer pageSize) {
        List<Document> result = esSearchUtil.fullSearch(page, pageSize);
        return JSON.toJSONString(result);
    }
}
