package com.alibaba.otter.shared.common.utils.jest;

import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.ClearScroll;
import io.searchbox.core.Delete;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.core.Update;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.aliases.AliasExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.params.Parameters;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by depu_lai on 2017/10/29.
 */
public class JestTemplate implements ElasticSearchDAO {

    private JestClient jestClient;

    public JestTemplate(DbMediaSource mediaSource) {
        JestClientFactory factory = new JestClientFactory();
        //根据外部传进来的url构建elasticsearch客户端
        String connectionUrl = mediaSource.getUrl();

        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(true)
                //enable host discovery
                .discoveryEnabled(true)
                .discoveryFrequency(1L, TimeUnit.MINUTES)
                //Per default this implementation will create no more than 2 concurrent connections per given route
                .defaultMaxTotalConnectionPerRoute(2)
                // and no more 20 connections in total
                .maxTotalConnection(20)
                .connTimeout(10000)
                .readTimeout(10000)
                .build());
        this.jestClient = factory.getObject();
    }


    public void close() {
        this.jestClient.shutdownClient();
        this.jestClient = null;
    }

    public JestClient getJestClient() {
        return this.jestClient;
    }


    @Override
    public boolean indicesExist(String indexName) {
        IndicesExists indicesExists = new IndicesExists.Builder(indexName).build();
        try {
            JestResult jestResult = jestClient.execute(indicesExists);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean aliasExist(String aliasName) {
        AliasExists aliasExists = new AliasExists.Builder().alias(aliasName).build();
        try {
            JestResult jestResult = jestClient.execute(aliasExists);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean typeExist(String indexName, String typeName) {
        //jest5.3.3版本TypeExist无法准确访问5.5.2版本的ES集群
        //TypeExist typeExist = new TypeExist.Builder(indexName).addType(typeName).build();
        GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).addType(typeName).build();
        try {
            JestResult jestResult = jestClient.execute(getMapping);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String health() {
        Health health = new Health.Builder().build();
        try {
            JestResult jestResult = jestClient.execute(health);
            return jestResult.getJsonObject().get("status").getAsString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "red";
    }

    public JsonObject getTableMapping(String indexName, String typeName) {
        GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).addType(typeName).build();
        try {
            JestResult jestResult = jestClient.execute(getMapping);
            JsonObject jsonObject = jestResult.getJsonObject();
            if (jestResult.isSucceeded()) {
                // ES数据源配置支持索引别名
                for (Map.Entry<String, JsonElement> e : jsonObject.entrySet()) {
                    JsonObject mappingJson = e.getValue().getAsJsonObject().getAsJsonObject("mappings").getAsJsonObject(typeName).getAsJsonObject("properties");
                    return mappingJson;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new JsonObject();
    }


    @Override
    public JestResult insertDoc(String index, String type, String id, String esParent, String doc) {
        JestResult result = null;
        try {
            Index.Builder indexBuilder = new Index.Builder(doc).index(index).type(type).id(id);
            if (!StringUtils.isBlank(esParent)) {
                indexBuilder.setParameter(Parameters.PARENT, esParent);
            }
            result = this.jestClient.execute(indexBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult updateDoc(String index, String type, String id,String esParent, String partialDoc) {
        JestResult result = null;
        try {
            Update.Builder updateBuilder = new Update.Builder(partialDoc).index(index).type(type).id(id);
            if (!StringUtils.isBlank(esParent)) {
                updateBuilder.setParameter(Parameters.PARENT, esParent);
            }
            result = this.jestClient.execute(updateBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public JestResult deleteDoc(String index, String type, String id, String esParent) {
        JestResult result = null;
        try {
            Delete.Builder deleteBuilder = new Delete.Builder(id).index(index).type(type);
            if (!StringUtils.isBlank(esParent)) {
                deleteBuilder.setParameter(Parameters.PARENT, esParent);
            }
            result = this.jestClient.execute(deleteBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public BulkResult bulkOperationDoc(List<BulkableAction> actions) {
        Bulk.Builder bulkBuilder = new Bulk.Builder();
        for (int i = 0, len = actions.size(); i < len; i++) {
            bulkBuilder.addAction(actions.get(i));
        }
        BulkResult bulkResult = null;
        try {
            bulkResult = this.jestClient.execute(bulkBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bulkResult;
    }


    @Override
    public boolean deleteType(String indexName, String typeName) {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).type(typeName).build();
        try {
            JestResult jestResult = this.jestClient.execute(deleteIndex);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


    @Override
    public boolean clearType(String index, String typeName) {
        //批量删除所有文档（每批2000条）
        Integer pageSize = 2000;
        Long scrollTimeInMillis = 100000L;
        String scrollTime = String.valueOf(scrollTimeInMillis / 1000) + "m";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(pageSize);
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(index)
                .addType(typeName)
                // .addSort(new Sort("code"))
                .setParameter(Parameters.SCROLL, scrollTime)
                .build();
        JestResult result = null;
        try {
            result = this.jestClient.execute(search);
        } catch (IOException e) {
            e.printStackTrace();
        }
        JsonArray hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
        //Scroll until no hits are returned
        do {
            List<BulkableAction> bulkDelete = new ArrayList<BulkableAction>();
            for (JsonElement hit : hits) {
                //Handle the hit... bulk delete
                String indexName = hit.getAsJsonObject().get("_index").getAsString();
                String indexType = hit.getAsJsonObject().get("_type").getAsString();
                String id = hit.getAsJsonObject().get("_id").getAsString();
                bulkDelete.add(new Delete.Builder(id).index(indexName).type(indexType)
                        .build());
            }
            //批量删除
            bulkOperationDoc(bulkDelete);
            String scrollId = result.getJsonObject().get("_scroll_id").getAsString();
            SearchScroll scroll = new SearchScroll.Builder(scrollId, scrollTime).build();
            try {
                result = this.jestClient.execute(scroll);
                // clear a single scroll id
                ClearScroll clearScroll = new ClearScroll.Builder().addScrollId(scrollId).build();
                this.jestClient.execute(clearScroll);
            } catch (IOException e) {
                e.printStackTrace();
            }
            hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
        } while (hits.size() != 0); // Zero hits mark the end of the scroll and the while loop.
        // clear all scroll ids
        ClearScroll clearScroll = new ClearScroll.Builder().build();
        try {
            result = this.jestClient.execute(clearScroll);
            return result.isSucceeded();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public JestResult getForDoc(String indexName, String typeName, String id, String routing){
        Get.Builder getBuilder = new Get.Builder(indexName, id).type(typeName);
        if (org.springframework.util.StringUtils.hasText(routing)) {
            getBuilder.setParameter(Parameters.ROUTING, routing);
        }
        JestResult result = null;
        try {
            result = jestClient.execute(getBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

}
