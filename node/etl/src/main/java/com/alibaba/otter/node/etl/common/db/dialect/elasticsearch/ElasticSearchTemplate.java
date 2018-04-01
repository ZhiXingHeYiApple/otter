package com.alibaba.otter.node.etl.common.db.dialect.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.common.utils.jest.JestTemplate;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by depu_lai on 2017/10/27.
 */
public class ElasticSearchTemplate implements NoSqlTemplate {
    private JestTemplate JestTemplate = null;
    private DateTimeFormatter formatter;

    public ElasticSearchTemplate(JestTemplate jestTemplate) {
        this.JestTemplate = jestTemplate;
        DateTimeParser[] parsers = {
                DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").getParser(),
                // timestamp和datetime类型
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
                // date类型时间
                DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
                // time类型时间
                DateTimeFormat.forPattern("HH:mm:ss").getParser(),
        };

        formatter = new DateTimeFormatterBuilder()
                .append(null, parsers)
                .toFormatter()
                .withZone(DateTimeZone.getDefault()); // DateTimeZone.forOffsetHours(8)
    }

    @Override
    public List<Integer> batchEventDatas(List<EventData> events) throws ConnClosedException {
        List<Integer> bulkResult = new ArrayList<Integer>(
                Collections.nCopies(events.size(), new Integer(0)));
        //bulk操作
        List<BulkableAction> actions = new ArrayList<BulkableAction>(events.size());
        for (int i = 0, len = events.size(); i < len; i++) {
            EventData eventData = events.get(i);
            String indexName = eventData.getSchemaName();
            String typeName = eventData.getTableName();
            //文档的ID
            String oldDocId = "";
            //Map<String,Object> oldKeyMap = new HashMap<String, Object>(eventData.getOldKeys().size());
            for (int j = 0, len2 = eventData.getOldKeys().size(); j < len2; j++) {
                EventColumn eventColumn = eventData.getOldKeys().get(j);
                oldDocId += eventColumn.getColumnValue();
                //oldKeyMap.put(eventColumn.getColumnName(),columnValueTransform(eventColumn));
            }
            String docId = "";
            Map<String, Object> keyMap = new HashMap<String, Object>(eventData.getKeys().size());
            for (int j = 0, len3 = eventData.getKeys().size(); j < len3; j++) {
                EventColumn eventColumn = eventData.getKeys().get(j);
                docId += eventColumn.getColumnValue();
                keyMap.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
            }
            //文档的内容
            Map<String, Object> docContent = new HashMap<String, Object>();
            for (int j = 0, len4 = eventData.getColumns().size(); j < len4; j++) {
                EventColumn eventColumn = eventData.getColumns().get(j);
                docContent.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
            }
            BulkableAction request = null;
            String esParent = null;
            switch (eventData.getEventType()) {
                case INSERT: {
                    docContent.putAll(keyMap);
                    esParent = (String) docContent.get("esParent");
                    Index.Builder indexBuilder = new Index.Builder(docContent).index(indexName).type(typeName).id(docId);
                    if (!StringUtils.isBlank(esParent)) {
                        indexBuilder.setParameter(Parameters.PARENT, esParent);
                    }
                    request = indexBuilder.build();
                    break;
                }
                case UPDATE: {
                    docContent.putAll(keyMap);
                    esParent = (String) docContent.get("esParent");
                    if (oldDocId.equals("") || oldDocId.equals(docId)) {
                        //只更新非主键字段
                        Index.Builder indexBuilder = new Index.Builder(docContent).index(indexName).type(typeName).id(docId);
                        if (!StringUtils.isBlank(esParent)) {
                            indexBuilder.setParameter(Parameters.PARENT, esParent);
                        }
                        request = indexBuilder.build();
                    } else {
                        Delete.Builder deleteBuilder = new Delete.Builder(docId).index(indexName).type(typeName);
                        if (!StringUtils.isBlank(esParent)) {
                            deleteBuilder.setParameter(Parameters.PARENT, esParent);
                        }
                        actions.add(deleteBuilder.build());
                        Index.Builder indexBuilder = new Index.Builder(docContent).index(indexName).type(typeName).id(docId);
                        if (!StringUtils.isBlank(esParent)) {
                            indexBuilder.setParameter(Parameters.PARENT, esParent);
                        }
                        request = indexBuilder.build();
                    }
                    break;
                }
                case DELETE: {
                    docContent.putAll(keyMap);
                    esParent = (String) docContent.get("esParent");
                    Delete.Builder deleteBuilder = new Delete.Builder(docId).index(indexName).type(typeName);
                    if (!StringUtils.isBlank(esParent)) {
                        deleteBuilder.setParameter(Parameters.PARENT, esParent);
                    }
                    request = deleteBuilder.build();
                    break;
                }
                default:
            }
            actions.add(request);
        }
        BulkResult result = this.JestTemplate.bulkOperationDoc(actions);
        //结果处理
        for (int i = 0, len = result.getItems().size(); i < len; i++) {
            BulkResult.BulkResultItem itemResult = result.getItems().get(i);
            if (StringUtils.isBlank(itemResult.error)) {//成功
                bulkResult.set(i, 1);
            } else {//失败
                bulkResult.set(i, 0);
            }
        }
        return bulkResult;
    }

    @Override
    public int insertEventData(EventData event) throws ConnClosedException {
        String indexName = event.getSchemaName();
        String typeName = event.getTableName();
        //文档的ID
        String docId = "";
        Map<String, Object> keyMap = new HashMap<String, Object>(event.getKeys().size());
        for (int j = 0, len3 = event.getKeys().size(); j < len3; j++) {
            EventColumn eventColumn = event.getKeys().get(j);
            docId += eventColumn.getColumnValue();
            keyMap.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
        }
        //文档的内容
        Map<String, Object> docContent = new HashMap<String, Object>();
        for (int j = 0, len4 = event.getColumns().size(); j < len4; j++) {
            EventColumn eventColumn = event.getColumns().get(j);
            docContent.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
        }
        docContent.putAll(keyMap);
        String esParent = (String) docContent.get("esParent");
        JestResult jestResult = this.JestTemplate.insertDoc(indexName, typeName, docId, esParent, JSON.toJSONString(docContent));
        return jestResult.isSucceeded() ? 1 : 0;
    }

    @Override
    public int updateEventData(EventData event) throws ConnClosedException {
        String indexName = event.getSchemaName();
        String typeName = event.getTableName();
        //文档的ID
        String oldDocId = "";
        //Map<String,Object> oldKeyMap = new HashMap<String, Object>(eventData.getOldKeys().size());
        for (int j = 0, len2 = event.getOldKeys().size(); j < len2; j++) {
            EventColumn eventColumn = event.getOldKeys().get(j);
            oldDocId += eventColumn.getColumnValue();
            //oldKeyMap.put(eventColumn.getColumnName(),columnValueTransform(eventColumn));
        }
        String docId = "";
        Map<String, Object> keyMap = new HashMap<String, Object>(event.getKeys().size());
        for (int j = 0, len3 = event.getKeys().size(); j < len3; j++) {
            EventColumn eventColumn = event.getKeys().get(j);
            docId += eventColumn.getColumnValue();
            keyMap.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
        }
        //文档的内容
        Map<String, Object> docContent = new HashMap<String, Object>();
        for (int j = 0, len4 = event.getColumns().size(); j < len4; j++) {
            EventColumn eventColumn = event.getColumns().get(j);
            docContent.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
        }
        docContent.putAll(keyMap);
        String esParent = (String) docContent.get("esParent");
        if (oldDocId.equals("") || oldDocId.equals(docId)) {
            //只更新非主键字段
            JestResult jestResult = this.JestTemplate.insertDoc(indexName, typeName, docId, esParent, JSON.toJSONString(docContent));
            return jestResult.isSucceeded() ? 1 : 0;
        } else {
            JestResult jestResult = this.JestTemplate.deleteDoc(indexName, typeName, oldDocId, esParent);
            if (!jestResult.isSucceeded()) {
                return 0;
            }
            jestResult = this.JestTemplate.insertDoc(indexName, typeName, docId, esParent, JSON.toJSONString(docContent));
            return jestResult.isSucceeded() ? 1 : 0;
        }
    }

    @Override
    public int deleteEventData(EventData event) throws ConnClosedException {
        String indexName = event.getSchemaName();
        String typeName = event.getTableName();
        //文档的ID
        String docId = "";
        for (int j = 0, len3 = event.getKeys().size(); j < len3; j++) {
            EventColumn eventColumn = event.getKeys().get(j);
            docId += eventColumn.getColumnValue();
        }
        String esParent = null;
        for (int j = 0, len = event.getColumns().size(); j < len; j++) {
            EventColumn eventColumn = event.getColumns().get(j);
            if ("esParent".equals(eventColumn.getColumnName())) {
                esParent = eventColumn.getColumnValue();
                break;
            }
        }
        JestResult jestResult = this.JestTemplate.deleteDoc(indexName, typeName, docId, esParent);
        return jestResult.isSucceeded() ? 1 : 0;
    }

    /**
     * 创建表
     *
     * @param event
     * @return
     * @throws ConnClosedException
     */
    @Override
    public EventData createTable(EventData event) throws ConnClosedException {

        return null;
    }

    @Override
    public EventData alterTable(EventData event) throws ConnClosedException {
        return null;
    }

    /**
     * 删表操作
     *
     * @param event
     * @return
     * @throws ConnClosedException
     */
    @Override
    public boolean eraseTable(EventData event) throws ConnClosedException {
        return this.JestTemplate.deleteType(event.getSchemaName(), event.getTableName());
    }

    /**
     * 清空表
     *
     * @param event
     * @return
     * @throws ConnClosedException
     */
    @Override
    public boolean truncateTable(EventData event) throws ConnClosedException {
        return this.JestTemplate.clearType(event.getSchemaName(), event.getTableName());
    }

    @Override
    public EventData renameTable(EventData event) throws ConnClosedException {
        return null;
    }


    private Object columnValueTransform(EventColumn eventColumn) {
        switch (eventColumn.getColumnType()) {
            case 4://int
                return Integer.valueOf(eventColumn.getColumnValue());
            case 12://string
                return eventColumn.getColumnValue();
            case 91:// date
            case 92:// time
                return eventColumn.getColumnValue();
            case 93:// datetime and timestamp
                String timeString = eventColumn.getColumnValue();
                if (StringUtils.isBlank(timeString)){
                    return eventColumn.getColumnValue();
                }else{
                    DateTime dateTime = formatter.parseDateTime(timeString);
                    return dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // Atom (ISO 8601)格式的时间
                }
            default:
                return eventColumn.getColumnValue();
        }
    }
}
