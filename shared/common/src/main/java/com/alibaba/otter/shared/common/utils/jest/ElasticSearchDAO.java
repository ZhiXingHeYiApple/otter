package com.alibaba.otter.shared.common.utils.jest;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.BulkResult;

import java.util.List;
import java.util.Map;

/**
 * Created by depu_lai on 2017/10/29.
 */
public interface ElasticSearchDAO {

    /**
     * 判断索引是否存在
     * @param indexName
     * @return
     */
    boolean indicesExist(String indexName);

    /**
     * 判断索引别名是否存在
     * @param aliasName
     * @return
     */
    boolean aliasExist(String aliasName);

    /**
     * 判断type是否存在
     * @param indexName 索引
     * @param typeName 类型
     * @return
     */
    boolean typeExist(String indexName, String typeName);

    /**
     * 查看集群健康信息
     * @return "red","yellow","green"
     */
    String health();

    /**
     * 索引一条文档
     * @param doc 文档内容
     * @return
     */
    JestResult insertDoc(String index, String type, String id, String esParent, String doc);

    /**
     * 更新一条文档
     * @param partialDoc 部分文档内容
     * @return
     */
    JestResult updateDoc(String index, String type, String id,String esParent,String partialDoc);

    /**
     * 根据id删除文档
     * @param id
     * @return
     */
    JestResult deleteDoc(String index, String type, String id, String esParent);

    /**
     * bulk操作，包含index、update、delete
     * @param actions
     * @return
     */
    BulkResult bulkOperationDoc(List<BulkableAction> actions);

    /**
     * 删除索引中的一个type（一张表）
     * @param indexName
     * @param typeName
     * @return
     */
    boolean deleteType(String indexName, String typeName);

    /**
     * 清空type内所有文档
     * @return
     */
    boolean clearType(String indexName, String typeName);


    JestResult getForDoc(String indexName, String typeName, String id, String routing);

}
