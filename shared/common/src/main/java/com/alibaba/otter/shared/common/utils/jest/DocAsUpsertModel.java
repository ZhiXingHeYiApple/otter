package com.alibaba.otter.shared.common.utils.jest;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * @author Depu Lai
 * @description
 * @company BestSign
 * @date 2018/4/9
 */
public class DocAsUpsertModel {
    @JSONField(ordinal = 2, name = "doc_as_upsert")
    private boolean docAsUpsert;
    @JSONField(ordinal = 1)
    private Object doc;

   public boolean isDocAsUpsert(){
       return docAsUpsert;
   }

   public void setDocAsUpsert(boolean docAsUpsert){
       this.docAsUpsert = docAsUpsert;
   }

   public Object getDoc(){
       return doc;
   }

   public void setDoc(Object doc){
       this.doc = doc;
   }
}
