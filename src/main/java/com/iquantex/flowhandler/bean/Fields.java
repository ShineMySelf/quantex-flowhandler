package com.iquantex.flowhandler.bean;

import java.util.Arrays;
import java.util.List;

public class Fields {

    private List<String> fields;
    public Fields(String... values){
        if (values.length==0){
            this.fields = Arrays.asList("default");
        }else {
            this.fields = Arrays.asList(values);
        }
    }
    public List<String> getFields() {
        return fields;
    }
}
