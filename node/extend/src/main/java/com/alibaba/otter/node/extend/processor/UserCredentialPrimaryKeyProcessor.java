package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * @author Depu Lai
 * @description
 * @company BestSign
 * @date 2018/5/10
 */
public class UserCredentialPrimaryKeyProcessor extends AbstractEventProcessor {

    @Override
    public boolean process(EventData eventData) {
        // 限制：user_id列是无法修改值的
        for (int i = 0; i < eventData.getColumns().size(); i++) {
            EventColumn column = eventData.getColumns().get(i);
            if ("user_id".equals(column.getColumnName())) {
                List<EventColumn> keys = eventData.getKeys();
                keys.add(column);
                eventData.setKeys(keys);
                // old keys
                List<EventColumn> oldKeys = eventData.getOldKeys();
                if (!CollectionUtils.isEmpty(oldKeys)) {// 存在主键变更时
                    oldKeys.add(column);
                    eventData.setOldKeys(oldKeys);
                }
                eventData.getColumns().remove(i);
                break;
            }
        }
        return true;
    }
}
