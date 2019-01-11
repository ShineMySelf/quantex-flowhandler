package com.iquantex.flowhandler.bean;

import java.util.Arrays;
import java.util.List;

public class Values {
    List<Object> values;
    public Values(Object... values){
        this.values = Arrays.asList(values);
    }
    public List<Object> getValues() {
        return values;
    }
}
