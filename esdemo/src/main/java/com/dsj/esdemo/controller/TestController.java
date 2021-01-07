package com.dsj.esdemo.controller;

import com.alibaba.fastjson.JSON;
import com.dsj.esdemo.model.Knowledge;
import com.dsj.esdemo.util.EsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class TestController {

    @Autowired
    public EsUtil esUtil;

    @PostMapping("/get")
    public void test(@RequestParam String index, @RequestParam String id) throws Exception {
        esUtil.get(index, id);
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
                .id(UUID.randomUUID().toString())
                .title(title)
                .content(content)
                .build();
        String jsonString = JSON.toJSONString(knowledge);
        System.out.println(jsonString);
        return esUtil.addDocument(index, knowledge.getId(), jsonString);
    }

    @PostMapping("/document/delete")
    public boolean deleteDocument(@RequestParam String index, @RequestParam String id) {
        return esUtil.deleteDocument(index, id);
    }
}
