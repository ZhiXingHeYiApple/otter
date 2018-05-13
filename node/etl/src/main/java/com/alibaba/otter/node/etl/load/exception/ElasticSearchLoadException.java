package com.alibaba.otter.node.etl.load.exception;

/**
 * Created by depu_lai on 2017/10/20.
 */

public class ElasticSearchLoadException extends RuntimeException {
    private String failDocs;
    private String errorInfo;


    public ElasticSearchLoadException(String failDocs, String errorInfo) {
        super(errorInfo + ": " + failDocs);
        this.failDocs = failDocs;
        this.errorInfo = errorInfo;
    }


    public String getErrorInfo() {
        return errorInfo;
    }

    public void setErrorInfo(String errorInfo) {
        this.errorInfo = errorInfo;
    }

    public String getFailDocs() {
        return failDocs;
    }

    public void setFailDocs(String failDocs) {
        this.failDocs = failDocs;
    }
}
