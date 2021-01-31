package com.dsj.esdemo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.log4j.Log4j2;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Log4j2
@Component
public class EsUtil {

    @Autowired
    private RestHighLevelClient client;
    @Autowired
    private EsSearchUtil esSearchUtil;

    private String getUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * 获取指定的文档
     * @param index 索引名称
     * @param id    文档id
     * @return JSONObject
     */
    public JSONObject getDocument(String index, String id) throws Exception {

        if (!isIndexExist(index)) {
            log.warn("不存在该索引，index：{}", index);
            return null;
        }

        GetRequest getRequest = new GetRequest(index, id);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> map = getResponse.getSource();
        if (map == null) {
            log.warn("不存在该文档，id：{}", id);
            return null;
        }

        return (JSONObject) JSON.toJSON(map);
    }

    /**
     * 判断索引是否存在
     * @param index 索引名称
     * @return  存在true，不存在false
     */
    private Boolean isIndexExist(String index) {
        boolean exists = false;
        GetIndexRequest request = new GetIndexRequest(index);

        try {
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("查询索引是否存在时发生异常：{}", e.getMessage());
        }

        return exists;
    }

    /**
     * 创建索引
     * @param index 索引名称
     * @return 成功true，失败false
     */
    public Boolean createIndex(String index) {
        String result = null;

        if (isIndexExist(index)) {
            log.warn("索引已存在!");
            return false;
        }

        try {
            CreateIndexRequest request = new CreateIndexRequest(index);
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            result = response.index();
        } catch (IOException e) {
            log.error("创建索引时发生异常：{}", e.getMessage());
        }

        if (index.equals(result)) {
            log.info("创建索引 {} 成功", index);
            return true;
        } else {
            log.info("创建索引 {} 失败", index);
            return false;
        }
    }

    /**
     * 删除索引（危险操作，将删除该索引下所有文档）
     * @param index 索引名称
     * @return  删除成功true，删除失败false
     */
    public Boolean deleteIndex(String index) {
        boolean result = false;
        if (!isIndexExist(index)) {
            log.error("索引 {} 不存在!", index);
            return false;
        }

        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            result = deleteResponse.isAcknowledged();
        } catch (IOException e) {
            log.warn("删除索引 {} 发生异常：{}", index, e.getMessage());
        }

        if (result) {
            log.info("删除索引 {} 成功", index);
            return true;
        } else {
            log.info("删除索引 {} 失败", index);
            return false;
        }
    }

    /**
     * 新增文档
     * @param index 索引名称
     * @param id    文档id
     * @param jsonString    json格式的文档内容
     * @return 新增成功true，新增失败false
     */
    public Boolean addDocument(String index, String id, String title, String jsonString) {
        String result = null;

        if (esSearchUtil.isDocumentTitleExist(index, title)){
            log.info("新增文档失败! 已存在 title：{} ", title);
            return false;
        }

        IndexRequest request = new IndexRequest(index)
                .id(id)
                .source(jsonString, XContentType.JSON);
        try {
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            result = response.getId();
        } catch (IOException e) {
            log.warn("新增文档发生异常：{}", e.getMessage());
        }

        if (id.equals(result)) {
            log.info("新增文档成功，id：{}", id);
            return true;
        } else {
            log.info("新增文档失败，id：{}", id);
            return false;
        }
    }

    /**
     * 删除文档
     * @param index 文档所属索引
     * @param id    文档id
     * @return  删除成功true，删除失败false
     */
    public Boolean deleteDocument(String index, String id) {
        String result = null;
        DeleteRequest deleteRequest = new DeleteRequest()
                .index(index)
                .id(id);
        try {
            DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            result = deleteResponse.getId();
        } catch (IOException e) {
            log.warn("删除文档发生异常：{}", e.getMessage());
        }

        if (id.equals(result)){
            log.info("删除文档 {} 成功", id);
            return true;
        } else {
            log.info("删除文档 {} 失败", id);
            return false;
        }
    }

}
