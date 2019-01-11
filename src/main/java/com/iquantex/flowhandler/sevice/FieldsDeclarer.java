package com.iquantex.flowhandler.sevice;


import com.iquantex.flowhandler.bean.Fields;

import java.util.Map;


public interface FieldsDeclarer {
    void delare(Fields fields);

    void delareStream(String streamId, Fields fields);

    Map<String, Fields> getFieldDeclare();


}
