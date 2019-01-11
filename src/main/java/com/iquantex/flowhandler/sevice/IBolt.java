package com.iquantex.flowhandler.sevice;



import com.iquantex.flowhandler.annotation.Stream;
import com.iquantex.flowhandler.bean.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface IBolt extends IComponent {

    void execute(Tuple tuple, Emitor emitor);

    default List<String> getSteamIds(){
        Stream annotation = this.getClass().getAnnotation(Stream.class);
        if (annotation!=null){
            String[] streamIds = annotation.streamIds();
            return Arrays.asList(streamIds);
        }
        return new ArrayList<>();
    }

}
