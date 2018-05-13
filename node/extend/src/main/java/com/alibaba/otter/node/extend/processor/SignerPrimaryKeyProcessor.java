package com.alibaba.otter.node.extend.processor;

import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Depu Lai
 * @description
 * @company BestSign
 * @date 2018/5/10
 */
public class SignerPrimaryKeyProcessor extends AbstractEventProcessor {
    @Override
    public boolean process(EventData eventData) {
        // 局限性，DDL除了新增字段，其他是不支持的，如果把唯一标识列id、contract_id、signer_user_id给删了或重命名，同步逻辑就完全错了
        // contract_id、signer_user_id这两列配合主键作为联合ID，是不能修改值的，否则在目的源中是无法准确定位需要操作的文档（导致异常或数据不一致）
        Map<String, EventColumn> keyMap = new HashMap<String, EventColumn>(2);
        int find = 0;
        for (int i = 0; i < eventData.getColumns().size(); i++) {
            EventColumn column = eventData.getColumns().get(i);
            if ("contract_id".equals(column.getColumnName())) {
                keyMap.put("contract_id", column);
                find++;
            }
            if ("signer_user_id".equals(column.getColumnName())) {
                keyMap.put("signer_user_id", column);
                find++;
            }
            if (find == 2) {
                break;
            }
        }

        List<EventColumn> keys = eventData.getKeys();
        keys.add(keyMap.get("contract_id"));
        keys.add(keyMap.get("signer_user_id"));
        eventData.setKeys(keys);

        // old keys
        List<EventColumn> oldKeys = eventData.getOldKeys();
        if (!CollectionUtils.isEmpty(oldKeys)) {// 存在主键变更时
            oldKeys.add(keyMap.get("contract_id"));
            oldKeys.add(keyMap.get("signer_user_id"));
            eventData.setOldKeys(oldKeys);
        }

        // eventData不能包含同名的重复字段，所以删除掉这两个字段，用下标删除比较适合删除一个元素，删2个逻辑是错的
        eventData.getColumns().remove(keyMap.get("contract_id"));
        eventData.getColumns().remove(keyMap.get("signer_user_id"));
        return true;
    }
}
