package com.iquantex.flowhandler.sevice;

import com.iquantex.flowhandler.bean.Fields;
import com.iquantex.flowhandler.bean.Tuple;
import com.iquantex.flowhandler.bean.Values;
import com.iquantex.flowhandler.sevice.impl.OutputFieldsDeclarer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Emitor {

    private IComponent iComponent;

    private String sourceComponent;

    public Emitor(IComponent iComponent, String sourceComponent){
        this.iComponent = iComponent;
        this.sourceComponent = sourceComponent;
    }

    public void emit(Values values){
        String streamId = "default";
        emit(streamId,values);
    }
    
    public void emit(String streamId, Values values){
        FieldsDeclarer declarer = this.iComponent.OutputFieldsDeclare(new OutputFieldsDeclarer());
        Map<String, Fields> fieldsMap = declarer.getFieldDeclare();
        Fields flowFields = fieldsMap.get(streamId);
        //没有申明这个outputField，直接返回
        if (flowFields==null){
            return;
        }
        Tuple tuple = new Tuple();
        tuple.setSourceComponent(sourceComponent);
        tuple.setStreamId(streamId);
        List<String> fields = flowFields.getFields();
        List<Object> valueList = values.getValues();
        Map<String,Object> fieldMap = new HashMap<>();
        for (int i = 0;i<fields.size();i++){
            if (valueList.size()<i+1){
                fieldMap.put(fields.get(i),null);
                continue;
            }
            fieldMap.put(fields.get(i),valueList.get(i));
        }
        tuple.setFieldMap(fieldMap);

        TupleMarket.putMsg(streamId,tuple);
    }
}
