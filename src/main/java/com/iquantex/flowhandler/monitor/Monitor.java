package com.iquantex.flowhandler.monitor;

import com.iquantex.flowhandler.bean.Fields;
import com.iquantex.flowhandler.bean.ReportMsg;
import com.iquantex.flowhandler.config.Config;
import com.iquantex.flowhandler.sevice.*;
import com.iquantex.flowhandler.sevice.impl.OutputFieldsDeclarer;
import com.iquantex.flowhandler.util.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Monitor {

    @Autowired
    private List<IBolt> iBolts;

    @Autowired
    private List<ISpout> iSpouts;

    @Autowired
    private Worker worker;

    private Map<String, Map<String,Integer>> spout2BoltMap = new ConcurrentHashMap<>();

    public void report(ReportMsg reportMsg){
        String source = reportMsg.getSource();
        String current = reportMsg.getCurrent();
        Map<String, Integer> boltMap = spout2BoltMap.get(source);
        ISpout iSpout = (ISpout) SpringContextUtil.getBean(toLowerCaseFirstOne(source));
        if (ReportMsg.SUCESS.equals(reportMsg.getStatus())){
            iSpout.ack(reportMsg);
        }else {
            iSpout.fail(reportMsg);
        }
        //限流检查开关
        if (Config.FLOW_ENABLED){
            if (current!=null){
                boltMap.put(current,reportMsg.getQueueSize());
                //检查是否需要限流
                checkBlock(source,current);
            }
        }

    }

    private void checkBlock(String source,String current){
        Map<String, Integer> boltTaskQueueMap = spout2BoltMap.get(source);
        if (boltTaskQueueMap==null){
            return;
        }
        int maxQueueSize = 1;
        for(Integer queueSize : boltTaskQueueMap.values()){
            //BoltTask中的queueSize有超过设定值，则让消息源线程阻塞
            if (queueSize>=maxQueueSize){
                maxQueueSize = queueSize;
            }
        }
        HashSet<String> spoutSet = getSpoutByBolt(current);
        if (maxQueueSize> Config.FLOW_QUEUE_SIZE){
            //阻塞
            changeSpoutTaskStatus(spoutSet,true);
        }else {
            //唤醒
            changeSpoutTaskStatus(spoutSet,false);
        }
    }

    private void changeSpoutTaskStatus(HashSet<String> spoutSet,boolean status){

        for (SpoutTask spoutTask : worker.getSpoutTasks()){
            ISpout iSpout = spoutTask.getiSpout();
            String spout = iSpout.getClass().getSimpleName();
            if (spoutSet.contains(spout)){
                if (status){
                    spoutTask.setStatus(status);
                }else {
                    spoutTask.notifyTask();
                }
            }
        }
    }

    private HashSet<String> getSpoutByBolt(String boltName){
        HashSet<String> spoutSet = new HashSet<>();
        for (String spout : spout2BoltMap.keySet()){
            Map<String, Integer> boltMap = spout2BoltMap.get(spout);
            for(String bolt : boltMap.keySet()){
                if (bolt.equals(boltName)){
                    spoutSet.add(spout);
                }
            }
        }
        return spoutSet;
    }

    public void prepare(){
        for (ISpout spout : iSpouts){
            String spoutName = spout.getClass().getSimpleName();
            Map<String,Integer> boltNumMap = new HashMap<>();
            spout2BoltMap.put(spoutName,boltNumMap);
            FieldsDeclarer fieldsDeclarer = spout.OutputFieldsDeclare(new OutputFieldsDeclarer());
            if (fieldsDeclarer==null){
                continue;
            }
            Map<String, Fields> fieldDeclare = fieldsDeclarer.getFieldDeclare();
            for (String streamId : fieldDeclare.keySet()){
                doRevesion(streamId,boltNumMap);
            }

        }
    }

    private void doRevesion(String streamId,Map<String,Integer> boltNumMap){
        for (IBolt bolt : iBolts){
            List<String> steamIds = bolt.getSteamIds();
            if (steamIds.contains(streamId)){
                String boltName = bolt.getClass().getSimpleName();
                boltNumMap.put(boltName,0);
            }else {
                continue;
            }
            FieldsDeclarer fieldsDeclarer = bolt.OutputFieldsDeclare(new OutputFieldsDeclarer());
            if (fieldsDeclarer==null){
                return;
            }
            Map<String, Fields> fieldDeclare = fieldsDeclarer.getFieldDeclare();
            if (fieldDeclare.isEmpty()){
                return;
            }
            for (String stream : fieldDeclare.keySet()){
                doRevesion(stream,boltNumMap);
            }
        }
    }

    private static String toLowerCaseFirstOne(String s){
        if(Character.isLowerCase(s.charAt(0)))
            return s;
        else
            return (new StringBuilder()).append(Character.toLowerCase(s.charAt(0))).append(s.substring(1)).toString();
    }



}
