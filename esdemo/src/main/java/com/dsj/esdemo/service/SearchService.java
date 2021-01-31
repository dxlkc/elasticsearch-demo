package com.dsj.esdemo.service;

import com.dsj.esdemo.model.Document;

import java.util.List;

public interface SearchService {

    List<Document> fuzzySearch(String[] index, String[] fields, String searchInfo, Integer page, Integer pageSize);

    List<Document> fullSearch(Integer page, Integer pageSize);
}
