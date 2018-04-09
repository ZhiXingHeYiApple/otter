package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

import java.sql.Types;
import java.util.Iterator;
import java.util.List;

/**
 * Created by depu_lai on 2017/11/16.
 */
public class MyEventProcessor extends AbstractEventProcessor {
    @Override
    public boolean process(EventData eventData) {
        // 实现自定义映射,如过滤掉某些字段、字段重命名、
        Iterator<EventColumn> it = eventData.getColumns().iterator();
        while (it.hasNext()) {
            EventColumn eventColumn = it.next();
            if ("data_update_time".equals(eventColumn.getColumnName()) || "data_create_time".equals(eventColumn.getColumnName())) {
                it.remove();
            }
        }

        // 构造新的字段
        EventColumn eventColumn = new EventColumn();
        eventColumn.setColumnValue("This is new field!");
        eventColumn.setColumnType(Types.VARCHAR);
        eventColumn.setColumnName("add_field");

        EventColumn eventColumn2 = new EventColumn();
        eventColumn2.setColumnValue(Math.random() > 0.5 ? "2" : "0");
        eventColumn2.setColumnType(Types.INTEGER);
        eventColumn2.setColumnName("rand_field");

        // 增加字段
        List<EventColumn> cols = eventData.getColumns();
        cols.add(eventColumn);
        cols.add(eventColumn2);
        eventData.setColumns(cols);
        // 继续处理该条数据
        if (eventColumn2.getColumnValue().equals("0")){
            return false;
        }else{
            return true;
        }
    }
}
