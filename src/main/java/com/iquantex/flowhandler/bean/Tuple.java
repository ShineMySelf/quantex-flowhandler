package com.iquantex.flowhandler.bean;

import java.util.Map;

public class Tuple {
    private String sourceComponent;
    private String streamId;
    private Map<String,Object> fieldMap;

    public Map<String, Object> getFieldMap() {
        return fieldMap;
    }

    public void setFieldMap(Map<String, Object> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public String getSourceComponent() {
        return sourceComponent;
    }

    public void setSourceComponent(String sourceComponent) {
        this.sourceComponent = sourceComponent;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public Object getValueByField(String field){
        return fieldMap.get(field);
    }

    public Object getValueByDefault(){
        return fieldMap.get("default");
    }
}
