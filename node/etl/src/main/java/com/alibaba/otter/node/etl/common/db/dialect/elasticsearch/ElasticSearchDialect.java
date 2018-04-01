package com.alibaba.otter.node.etl.common.db.dialect.elasticsearch;

import com.alibaba.otter.node.etl.common.db.dialect.AbstractDbDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.shared.common.utils.jest.JestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

/**
 * Created by depu_lai on 2017/10/27.
 */
public class ElasticSearchDialect extends AbstractDbDialect {
    // TODO: 2018/3/31 depu_lai
    /**
     * ElasticSearch的客户端
     */
    protected NoSqlTemplate noSqlTemplate;

    public ElasticSearchDialect(JestTemplate jestTemplate,LobHandler lobHandler) {
        super(jestTemplate,lobHandler);
        //实例化 ElasticSearchTemplate
        this.noSqlTemplate = new ElasticSearchTemplate(jestTemplate);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public String getDefaultSchema() {
        return null;
    }

    @Override
    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public boolean isCharSpacePadded() {
        return false;
    }

    @Override
    public boolean isCharSpaceTrimmed() {
        return false;
    }

    @Override
    public boolean isEmptyStringNulled() {
        return false;
    }

    @Override
    public boolean isSupportMergeSql() {
        return false;
    }

    @Override
    public boolean isDRDS() {
        return false;
    }

    @Override
    public boolean isNoSqlDB() {
        return true;
    }

/*    @Override
    public LobHandler getLobHandler() {
        return null;
    }*/

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return null;
    }

/*    @Override
    public TransactionTemplate getTransactionTemplate() {
        return null;
    }*/

    /**
     * 重写AbstractDbDialect中返回sqlTemplate的方法
     * @param <T>
     * @return
     */
    @Override
    public <T> T getSqlTemplate() {
        return (T)this.noSqlTemplate;
    }

/*
    @Override
    public Table findTable(String schema, String table) {
        return null;
    }

    @Override
    public Table findTable(String schema, String table, boolean useCache) {
        return null;
    }

    @Override
    public String getShardColumns(String schema, String table) {
        return null;
    }

    @Override
    public void reloadTable(String schema, String table) {

    }

    @Override
    public void destory() {

    }*/
}
