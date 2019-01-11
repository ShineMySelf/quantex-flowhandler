package com.iquantex.flowhandler.sevice.impl;


import com.iquantex.flowhandler.bean.Fields;
import com.iquantex.flowhandler.sevice.FieldsDeclarer;

import java.util.HashMap;
import java.util.Map;

public class OutputFieldsDeclarer implements FieldsDeclarer {
    private Map<String, Fields> map = new HashMap<>();

    @Override
    public void delare(Fields fields) {
        map.put("default",fields);
    }

    @Override
    public void delareStream(String streamId, Fields flowFields) {
        if (!map.containsKey(streamId)){
            map.put(streamId,flowFields);
        }
    }

    @Override
    public Map<String, Fields> getFieldDeclare() {
        return this.map;
    }
}
