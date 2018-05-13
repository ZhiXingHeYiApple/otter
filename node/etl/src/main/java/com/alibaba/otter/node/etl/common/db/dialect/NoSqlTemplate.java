package com.alibaba.otter.node.etl.common.db.dialect;

import com.alibaba.otter.node.etl.load.exception.ElasticSearchLoadException;
import com.alibaba.otter.shared.etl.model.EventData;

import java.util.List;

/**
 * Created by depu_lai on 2017/10/20.
 */
public interface NoSqlTemplate {
    /**
     * 批量执行dml数据操作，增，删，改
     *
     * @param events
     * @return 执行失败的记录集合返回, 失败原因消息保存在exeResult字段中
     */
    List<Integer> batchEventDatas(List<EventData> events) throws ElasticSearchLoadException;

    /**
     * 插入行数据
     *
     * @param event
     * @return 记录返回, 失败原因消息保存在exeResult字段中
     */
    int insertEventData(EventData event) throws ElasticSearchLoadException;

    /**
     * 更新行数句
     *
     * @param event
     * @return 记录返回, 失败原因消息保存在exeResult字段中
     */
    int updateEventData(EventData event) throws ElasticSearchLoadException;

    /**
     * 删除记录
     *
     * @param event
     * @return 记录返回, 失败原因消息保存在exeResult字段中
     */
    int deleteEventData(EventData event) throws ElasticSearchLoadException;

    /**
     * 建立表
     *
     * @param event
     * @return
     */
    EventData createTable(EventData event) throws ElasticSearchLoadException;

    /**
     * 修改表
     *
     * @param event
     * @return
     */
    EventData alterTable(EventData event) throws ElasticSearchLoadException;

    /**
     * 删除表
     *
     * @param event
     * @return
     */
    boolean eraseTable(EventData event) throws ElasticSearchLoadException;

    /**
     * 清空表
     *
     * @param event
     * @return
     */
    boolean truncateTable(EventData event) throws ElasticSearchLoadException;

    /**
     * 改名表
     *
     * @param event
     * @return
     */
    EventData renameTable(EventData event) throws ElasticSearchLoadException;
}
