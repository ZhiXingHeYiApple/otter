package com.alibaba.otter.node.etl.common.db.dialect.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.load.exception.ElasticSearchLoadException;
import com.alibaba.otter.node.etl.load.exception.LoadException;
import com.alibaba.otter.shared.common.utils.jest.DocAsUpsertModel;
import com.alibaba.otter.shared.common.utils.jest.JestTemplate;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestResult;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Update;
import io.searchbox.params.Parameters;
import javafx.util.Pair;
import org.apache.commons.collections.collection.SynchronizedCollection;
import org.apache.commons.collections.list.SynchronizedList;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by depu_lai on 2017/10/27.
 */
public class ElasticSearchTemplate implements NoSqlTemplate {

    class EventDataTransformToBulkRequest implements Callable {
        /**
         * 无序完成，需要通过index重新确定位置
         */
        private int index;

        /**
         * 线程的输入参数，采用构造传参
         */
        private EventData eventData;

        /**
         * 用于update事件主键发生变更时进行缓存，方便后续删除操作
         */
        private List<Pair> sharedDeleteList;

        /**
         * 用于update事件处理时，反查ES确认老ID文档是否存在的ES客户端
         */
        private JestTemplate jestTemplate;

        public EventDataTransformToBulkRequest(int index, EventData eventData, List<Pair> deleteList, JestTemplate jestTemplate) {
            this.index = index;
            this.eventData = eventData;
            this.sharedDeleteList = deleteList;
            this.jestTemplate = jestTemplate;
        }

        private BulkableAction toInsertRequest(EventData eventData) {
            String indexName = eventData.getSchemaName();
            String typeName = eventData.getTableName();
            String docId = getDocId(eventData);
            Map<String, Object> docContent = getDocContent(eventData);
            String esParent = (String) docContent.get("esParent");
            Index.Builder indexBuilder = new Index.Builder(docContent).index(indexName).type(typeName).id(docId);
            if (!StringUtils.isBlank(esParent)) {
                indexBuilder.setParameter(Parameters.PARENT, esParent);
            }
            return indexBuilder.build();
        }

        private BulkableAction toUpdateRequest(EventData eventData) {
            String indexName = eventData.getSchemaName();
            String typeName = eventData.getTableName();
            String docId = getDocId(eventData);
            String oldDocId = getOldDocId(eventData);
            Map<String, Object> docContent = getDocContent(eventData);
            String esParent = (String) docContent.get("esParent");
            if (eventData.getSyncMode().isField()) {// 列模式
                if (oldDocId.equals("") || oldDocId.equals(docId)) {// updte_upsert
                    DocAsUpsertModel docAsUpsertModel = new DocAsUpsertModel();
                    docAsUpsertModel.setDocAsUpsert(true);
                    docAsUpsertModel.setDoc(docContent);
                    Update.Builder updateBuilder = new Update.Builder(JSON.toJSONString(docAsUpsertModel)).index(indexName).type(typeName).id(docId);
                    if (!StringUtils.isBlank(esParent)) {
                        updateBuilder.setParameter(Parameters.PARENT, esParent);
                    }
                    return updateBuilder.build();
                } else {// 先删除再update_upsert
                    String routing = StringUtils.isBlank(esParent) ? null : esParent;
                    // 根据老id找到文档(这里要同步等待查询结果，效率比较低)
                    JestResult searchResult = this.jestTemplate.getForDoc(indexName, typeName, oldDocId, routing);
                    if (searchResult == null) {// IO Exception
                        //return bulkResult;
                        throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                    }
                    // 有响应结果的情形
                    if (Boolean.valueOf(true).equals(searchResult.getValue("found"))) {
                        Map<String, Object> oldDocSource = searchResult.getSourceAsObject(Map.class);
                        // 新、老文档合并
                        oldDocSource.putAll(docContent);
                        // 缓存老id的文档，以便后续删除
                        this.sharedDeleteList.set(this.index, new Pair(oldDocId, esParent));
                        // 索引合并后的新文档
                        Index.Builder indexBuilder = new Index.Builder(oldDocSource).index(indexName).type(typeName).id(docId);
                        if (!StringUtils.isBlank(esParent)) {
                            indexBuilder.setParameter(Parameters.PARENT, esParent);
                        }
                        return indexBuilder.build();
                    } else {// ES中找不到老主键对应的文档
                        DocAsUpsertModel docAsUpsertModel = new DocAsUpsertModel();
                        docAsUpsertModel.setDocAsUpsert(true);
                        docAsUpsertModel.setDoc(docContent);
                        Update.Builder updateBuilder = new Update.Builder(JSON.toJSONString(docAsUpsertModel)).index(indexName).type(typeName).id(docId);
                        if (!StringUtils.isBlank(esParent)) {
                            updateBuilder.setParameter(Parameters.PARENT, esParent);
                        }
                        return updateBuilder.build();
                    }
                }
            } else {// row模式
                if (oldDocId.equals("") || oldDocId.equals(docId)) {
                    //只更新非主键字段
                    Index.Builder indexBuilder = new Index.Builder(docContent).index(indexName).type(typeName).id(docId);
                    if (!StringUtils.isBlank(esParent)) {
                        indexBuilder.setParameter(Parameters.PARENT, esParent);
                    }
                    return indexBuilder.build();
                } else {
                    // 缓存老id的文档，以便后续删除
                    this.sharedDeleteList.set(index, new Pair(oldDocId, esParent));
                    Index.Builder indexBuilder = new Index.Builder(docContent).index(indexName).type(typeName).id(docId);
                    if (!StringUtils.isBlank(esParent)) {
                        indexBuilder.setParameter(Parameters.PARENT, esParent);
                    }
                    return indexBuilder.build();
                }
            }
        }

        private BulkableAction toDeleteRequest(EventData eventData) {
            String indexName = eventData.getSchemaName();
            String typeName = eventData.getTableName();
            String docId = getDocId(eventData);
            Map<String, Object> docContent = getDocContent(eventData);
            String esParent = (String) docContent.get("esParent");
            Delete.Builder deleteBuilder = new Delete.Builder(docId).index(indexName).type(typeName);
            if (!StringUtils.isBlank(esParent)) {
                deleteBuilder.setParameter(Parameters.PARENT, esParent);
            }
            return deleteBuilder.build();
        }

        @Override
        public Object call() throws Exception {
            BulkableAction request = null;
            switch (eventData.getEventType()) {
                case INSERT:
                    request = toInsertRequest(eventData);
                    break;
                case UPDATE:
                    request = toUpdateRequest(eventData);
                    break;
                case DELETE:
                    request = toDeleteRequest(eventData);
                    break;
            }
            return new Pair<Integer, BulkableAction>(this.index, request);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchTemplate.class);
    private JestTemplate jestTemplate = null;
    private static DateTimeFormatter formatter;
    // "MD5";
    private static final String ALGORITHM = "SHA1";

    // CachedThreadPool线程池在执行大量短生命周期的异步任务时（many short-lived asynchronous task），可以显著提高程序性能
    private static ExecutorService executor = Executors.newCachedThreadPool();

    public ElasticSearchTemplate(JestTemplate jestTemplate) {
        this.jestTemplate = jestTemplate;
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
    public List<Integer> batchEventDatas(List<EventData> events) throws ElasticSearchLoadException {
        System.out.println("###batchEventDatas" + events.size() + "###" + JSON.toJSONString(events));
        CompletionService<Pair<Integer, BulkableAction>> completionService = new ExecutorCompletionService<Pair<Integer, BulkableAction>>(executor);

        // 主键发生改变的部分文档更新需要删除老文档，多线程共享的集合
        List<Pair> deleteList = Collections.synchronizedList(Arrays.asList(new Pair[events.size()]));
        // 异步任务数
        int n = events.size();
        long startTime = System.currentTimeMillis();
        for (int i = 0, len = n; i < len; i++) {
            completionService.submit(new EventDataTransformToBulkRequest(i, events.get(i), deleteList, this.jestTemplate));
        }
        // java6 线程池异步执行
        int received = 0;
        boolean errors = false;
        // List<BulkableAction> actions = new ArrayList<BulkableAction>(events.size());
        List<BulkableAction> actions = Arrays.asList(new BulkableAction[events.size()]);
        while (received < n && !errors) {
            try {
                Future<Pair<Integer, BulkableAction>> resultFuture = completionService.take(); //blocks if none available
                Pair<Integer, BulkableAction> requestPair = resultFuture.get();
                BulkableAction request = requestPair.getValue();
                actions.set(requestPair.getKey(), request);
                received++;
            } catch (Exception e) {
                errors = true;
                // log
                logger.error("## EventDataTransformToBulkRequest error , [errorInfo={}]",
                        new Object[]{e.getMessage()});
                throw new ElasticSearchLoadException("EventDataTransformToBulkRequest error", e.getMessage());
            }
        }
        logger.info("EventDatas {} -> BulkRequests spend {} ms", events.size(), System.currentTimeMillis() - startTime);
        List<Integer> bulkResult = bulkToElasticSearch(actions, events, deleteList);
        return bulkResult;
    }

    @Override
    public int insertEventData(EventData event) throws ElasticSearchLoadException {
        String indexName = event.getSchemaName();
        String typeName = event.getTableName();
        String docId = getDocId(event);
        Map<String, Object> docContent = getDocContent(event);
        String esParent = (String) docContent.get("esParent");
        String docStr = JSON.toJSONString(docContent);
        JestResult jestResult = this.jestTemplate.insertDoc(indexName, typeName, docId, esParent, docStr);
        if (jestResult == null) {
            throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
        }
        if (!jestResult.isSucceeded()) {
            throw new ElasticSearchLoadException(docStr, jestResult.getErrorMessage());
        }
        return 1;
    }

    @Override
    public int updateEventData(EventData event) throws ElasticSearchLoadException {
        String indexName = event.getSchemaName();
        String typeName = event.getTableName();
        String docId = getDocId(event);
        String oldDocId = getOldDocId(event);
        Map<String, Object> docContent = getDocContent(event);
        String esParent = (String) docContent.get("esParent");
        if (event.getSyncMode().isField()) {// 列模式
            if (oldDocId.equals("") || oldDocId.equals(docId)) {
                DocAsUpsertModel docAsUpsertModel = new DocAsUpsertModel();
                docAsUpsertModel.setDocAsUpsert(true);
                docAsUpsertModel.setDoc(docContent);
                String partialDocStr = JSON.toJSONString(docAsUpsertModel);
                // 注意：列模式时，esParent对应的字段不修改的话在EventData中会缺失，无法支持父子文档，目前没有好的解决方式，建议使用ROW方式吧
                JestResult jestResult = this.jestTemplate.updateDoc(indexName, typeName, docId, esParent, partialDocStr);
                if (jestResult == null) {
                    throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                }
                if (!jestResult.isSucceeded()) {
                    throw new ElasticSearchLoadException(partialDocStr, jestResult.getErrorMessage());
                }
                return 1;
            } else {// 主键发生改变
                String routing = StringUtils.isBlank(esParent) ? null : esParent;
                // 根据老id找到文档(这里要同步等待查询结果，效率比较低)
                JestResult searchResult = this.jestTemplate.getForDoc(indexName, typeName, oldDocId, routing);
                if (searchResult == null) {// IO Exception，让同步任务挂起，避免binlog event丢失
                    throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                }
                // 在极端情况下（如主从切换时），会有部分binlog event重复消费，存在一定概率导致这里根据老的主键是找不到文档的
                if (Boolean.valueOf(true).equals(searchResult.getValue("found"))) {
                    Map<String, Object> oldDocSource = searchResult.getSourceAsObject(Map.class);
                    // 新、老文档合并
                    oldDocSource.putAll(docContent);
                    // 重新索引合并后的文档
                    JestResult jestResult = jestTemplate.insertDoc(indexName, typeName, docId, esParent, JSON.toJSONString(oldDocSource));
                    if (jestResult == null) {
                        throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                    }
                    if (jestResult.isSucceeded()) {
                        // 删除老id对应的文档
                        JestResult deleteResult = jestTemplate.deleteDoc(indexName, typeName, oldDocId, esParent);
                        if (deleteResult == null) {
                            throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                        }
                        if (!deleteResult.isSucceeded()) {
                            throw new ElasticSearchLoadException(oldDocId, jestResult.getErrorMessage());
                        }
                        return 1;
                    } else {
                        throw new ElasticSearchLoadException(oldDocId, jestResult.getErrorMessage());
                    }
                } else {
                    // 当根据该binlog event中oldKey在ES中找不到该文档时，说明这条binlog之前被消费成功过了，直接返回1
                    return 1;
                }
            }
        } else {// row模式
            if (oldDocId.equals("") || oldDocId.equals(docId)) {
                //只更新非主键字段
                String partialDocStr = JSON.toJSONString(docContent);
                JestResult jestResult = this.jestTemplate.insertDoc(indexName, typeName, docId, esParent, partialDocStr);
                if (jestResult == null) {
                    throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                }
                if (!jestResult.isSucceeded()) {
                    throw new ElasticSearchLoadException(partialDocStr, jestResult.getErrorMessage());
                }
                return 1;
            } else {
                JestResult deleteResult = this.jestTemplate.deleteDoc(indexName, typeName, oldDocId, esParent);
                if (deleteResult == null) {
                    throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                }
                if (deleteResult.isSucceeded()) {
                    String partialDocStr = JSON.toJSONString(docContent);
                    JestResult jestResult = this.jestTemplate.insertDoc(indexName, typeName, docId, esParent, partialDocStr);
                    if (jestResult == null) {
                        throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
                    }
                    if (!jestResult.isSucceeded()) {
                        throw new ElasticSearchLoadException(partialDocStr, jestResult.getErrorMessage());
                    }
                    return 1;
                } else {
                    throw new ElasticSearchLoadException(oldDocId, deleteResult.getErrorMessage());
                }
            }
        }
    }

    @Override
    public int deleteEventData(EventData event) throws ElasticSearchLoadException {
        String indexName = event.getSchemaName();
        String typeName = event.getTableName();
        String docId = getDocId(event);
        String esParent = null;
        for (int j = 0, len = event.getColumns().size(); j < len; j++) {
            EventColumn eventColumn = event.getColumns().get(j);
            if ("esParent".equals(eventColumn.getColumnName())) {
                esParent = eventColumn.getColumnValue();
                break;
            }
        }
        JestResult jestResult = this.jestTemplate.deleteDoc(indexName, typeName, docId, esParent);
        if (jestResult == null) {
            throw new ElasticSearchLoadException(null, "Can't connect to ES. Please check the config of connection");
        }
        if (!jestResult.isSucceeded()) {
            throw new ElasticSearchLoadException(docId, jestResult.getErrorMessage());
        }
        return 1;
    }

    /**
     * 创建表
     *
     * @param event
     * @return
     * @throws ElasticSearchLoadException
     */
    @Override
    public EventData createTable(EventData event) throws ElasticSearchLoadException {

        return null;
    }

    @Override
    public EventData alterTable(EventData event) throws ElasticSearchLoadException {
        return null;
    }

    /**
     * 删表操作
     *
     * @param event
     * @return
     * @throws ElasticSearchLoadException
     */
    @Override
    public boolean eraseTable(EventData event) throws ElasticSearchLoadException {
        return this.jestTemplate.deleteType(event.getSchemaName(), event.getTableName());
    }

    /**
     * 清空表
     *
     * @param event
     * @return
     * @throws ElasticSearchLoadException
     */
    @Override
    public boolean truncateTable(EventData event) throws ElasticSearchLoadException {
        return this.jestTemplate.clearType(event.getSchemaName(), event.getTableName());
    }

    @Override
    public EventData renameTable(EventData event) throws ElasticSearchLoadException {
        return null;
    }


    private static Object columnValueTransform(EventColumn eventColumn) {
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
                if (StringUtils.isBlank(timeString)) {
                    return eventColumn.getColumnValue();
                } else {
                    DateTime dateTime = formatter.parseDateTime(timeString);
                    return dateTime.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // Atom (ISO 8601)格式的时间
                }
            default:
                return eventColumn.getColumnValue();
        }
    }

    private static String getOldDocId(EventData eventData) {
        // update操作才会关注old 文档id
        String oldDocId = "";
        for (int j = 0, len2 = eventData.getOldKeys().size(); j < len2; j++) {
            EventColumn eventColumn = eventData.getOldKeys().get(j);
            oldDocId += eventColumn.getColumnValue();
            oldDocId += "#";
        }
        // SHA-1得到唯一ID
        if (eventData.getOldKeys().size() == 1) {
            oldDocId = oldDocId.substring(0, oldDocId.length() - 1);
        } else if (eventData.getOldKeys().size() > 1) {
            oldDocId = IdUtil.encode(ALGORITHM, oldDocId.substring(0, oldDocId.length() - 1));
        }
        return oldDocId;
    }

    private static String getDocId(EventData eventData) {
        //文档的ID
        String docId = "";
        for (int j = 0, len3 = eventData.getKeys().size(); j < len3; j++) {
            EventColumn eventColumn = eventData.getKeys().get(j);
            docId += eventColumn.getColumnValue();
            docId += "#";
        }
        // SHA-1得到唯一ID
        if (eventData.getKeys().size() == 1) {// 单个主键无需hash处理
            docId = docId.substring(0, docId.length() - 1);
        } else if (eventData.getKeys().size() > 1) {
            docId = IdUtil.encode(ALGORITHM, docId.substring(0, docId.length() - 1));
        }
        return docId;
    }

    private static Map<String, Object> getDocContent(EventData eventData) {
        Map<String, Object> keyMap = new HashMap<String, Object>(eventData.getKeys().size());
        for (int j = 0, len3 = eventData.getKeys().size(); j < len3; j++) {
            EventColumn eventColumn = eventData.getKeys().get(j);
            keyMap.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
        }
        //文档的内容
        final Map<String, Object> docContent = new HashMap<String, Object>();
        for (int j = 0, len4 = eventData.getColumns().size(); j < len4; j++) {
            EventColumn eventColumn = eventData.getColumns().get(j);
            docContent.put(eventColumn.getColumnName(), columnValueTransform(eventColumn));
        }
        docContent.putAll(keyMap);
        return docContent;
    }


    private List<Integer> bulkToElasticSearch(List<BulkableAction> actions, List<EventData> events, List<Pair> sharedDeleteList) {
        List<Integer> bulkResult = new ArrayList<Integer>(
                Collections.nCopies(events.size(), new Integer(0)));
        //bulk操作
        long bulkStartTime = System.currentTimeMillis();
        BulkResult result = this.jestTemplate.bulkOperationDoc(actions);
        logger.info("The bulk request spend {} ms", System.currentTimeMillis() - bulkStartTime);
        // bulk请求发生 IOException，如网络中断
        if (result == null) {
            // return bulkResult; 这样的话不会进行重试了，会漏数据
            throw new ElasticSearchLoadException(null, "Bulk operation happen IO exception. So temporality can't connect to ES.");
        }
        // HTTP层面的错误时，例如发生json格式异常时，result.getFailedItems().size()为零,部分文档失败时，result.getFailedItems().size()才会大于0
        if (!StringUtils.isBlank(result.getErrorMessage())) {
            // json格式异常
            // 部分失败了 One or more of the items in the Bulk request failed, check BulkResult.getItems() for more information.
            throw new ElasticSearchLoadException(JSON.toJSONString(result.getFailedItems()), result.getErrorMessage());
        }
        //结果处理
        for (int i = 0, len = result.getItems().size(); i < len; i++) {
            BulkResult.BulkResultItem itemResult = result.getItems().get(i);
            if (StringUtils.isBlank(itemResult.error)) {//成功
                if (sharedDeleteList.get(i) != null) {// 业务上主键变更不可能很频繁；不能使用异步请求ES，效率较低
                    String indexName = events.get(i).getSchemaName();
                    String typeName = events.get(i).getTableName();
                    JestResult deleteResult = jestTemplate.deleteDoc(indexName, typeName, (String) sharedDeleteList.get(i).getKey(), (String) sharedDeleteList.get(i).getValue());
                    // 发生IO Exception时，响应结果deleteResult为null
                    if (deleteResult != null && deleteResult.isSucceeded()) {
                        bulkResult.set(i, 1);
                    } else {
                        // 日志记录
//                        logger.error("## Delete old document fail when primary key changed, [operation={}, index={}, type={}, _id={},_parent={}, errorInfo={}]",
//                                new Object[]{itemResult.operation, itemResult.index, itemResult.type, itemResult.id, sharedDeleteList.get(i).getValue(), itemResult.error});
                        throw new ElasticSearchLoadException(JSON.toJSONString(itemResult), "Delete old document [" + sharedDeleteList.get(i).getKey() + ", " + sharedDeleteList.get(i).getValue() + "] fail when primary key changed");
                    }
                } else {
                    bulkResult.set(i, 1);
                }
            } /*else {//失败
                // 打印错误日志，以便数据丢失时方便排查问题
                logger.error("## Load ElasticSearch error , [operation={}, index={}, type={}, id={}, errorInfo={}]",
                        new Object[] { itemResult.operation, itemResult.index, itemResult.type,itemResult.id,itemResult.error });
                bulkResult.set(i, 0);
            }*/
        }
        return bulkResult;
    }
}
