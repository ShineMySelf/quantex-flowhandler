package com.iquantex.flowhandler.sevice;

import com.iquantex.flowhandler.annotation.Stream;
import com.iquantex.flowhandler.annotation.Parallel;
import com.iquantex.flowhandler.bean.Tuple;
import com.iquantex.flowhandler.monitor.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

@Component
public class Worker {

    private final static Logger LOG = LoggerFactory.getLogger(Worker.class);

    @Autowired
    private List<IBolt> iBolts;

    @Autowired
    private List<ISpout> iSpouts;

    @Autowired
    private Monitor monitor;

    private List<SpoutTask> spoutTasks = new CopyOnWriteArrayList<>();

    private Map<String,List<BoltTask>> boltTaskMap = new HashMap<>();

    public void startup(){
        //监控中心的初始化
        monitor.prepare();

        //初始化inputMessage与boltTask的mapping关系
        initBoltTaskMap();

        //消息源获取消息
        notifyISpouts();

        //路由TupleMarket中的Tuple到各自的BoltTask的消费队列中
        routeTuple();

    }

    public void routeTuple(){
        Executors.newSingleThreadExecutor().submit(new Thread(() -> {
            while (true){
                //获取消费节点订阅的事件集合
                Set<String> steamIds = getSteamIds();
                for (String steamId : steamIds){
                    Tuple msg = TupleMarket.pollMsg(steamId);
                    if (msg!=null){
                        notifyIBolts(steamId,msg);
                    }
                }
            }
        }));
    }

    private Set<String> getSteamIds(){
        Set<String> streamIdSet = new HashSet<>();
        List<IBolt> iBolts = getIBolts();
        for (IBolt iBolt : iBolts){
            List<String> steamIds = iBolt.getSteamIds();
            streamIdSet.addAll(steamIds);
        }
        return streamIdSet;
    }

    private void notifyIBolts(String steamId, Tuple tuple) {
        List<IBolt> iBolts = getIBolts();
        for(IBolt iBolt : iBolts){
            Stream annotation = iBolt.getClass().getAnnotation(Stream.class);
            if (annotation == null) {
                continue;
            }
            String[] streamIds = annotation.streamIds();
            List<String> steamIdList = Arrays.asList(streamIds);
            if (steamIdList.contains(steamId)){
                List<BoltTask> boltTaskList = boltTaskMap.get(steamId);
                String boltName = iBolt.getClass().getSimpleName();
                for(BoltTask boltTask : boltTaskList){
                    String boltTaskName = boltTask.getFlowNode().getClass().getSimpleName();
                    if (boltTaskName.equals(boltName)){
                        boltTask.putMsg(tuple);
                    }
                }
            }
        }
    }

    private void initBoltTaskMap(){
        List<IBolt> iBolts = getIBolts();
        for (IBolt iBolt : iBolts){
            Stream stream = iBolt.getClass().getAnnotation(Stream.class);
            if (stream == null) {
                continue;
            }
            int boltParallel = 1;
            Parallel parallel = iBolt.getClass().getAnnotation(Parallel.class);
            if (parallel!=null){
                boltParallel = parallel.parallel();
            }
            String[] streamIds = stream.streamIds();
            List<String> streamIdList = Arrays.asList(streamIds);
            for (String streamId : streamIdList){
                List<BoltTask> boltTasks = boltTaskMap.get(streamId);
                if (boltTasks==null){
                    BoltTask boltTask = new BoltTask(iBolt,monitor,boltParallel);
                    boltTask.start();
                    List<BoltTask> boltTaskList = new ArrayList<>();
                    boltTaskList.add(boltTask);
                    boltTaskMap.put(streamId,boltTaskList);

                }else {
                    String boltName = iBolt.getClass().getSimpleName();
                    List<String> boltTaskNameList = new ArrayList<>();
                    for (BoltTask flowTask : boltTasks){
                        String boltTasikName = flowTask.getFlowNode().getClass().getSimpleName();
                        boltTaskNameList.add(boltTasikName);

                    }
                    if (!boltTaskNameList.contains(boltName)){
                        BoltTask boltTask = new BoltTask(iBolt,monitor,boltParallel);
                        boltTask.start();
                        boltTasks.add(boltTask);
                    }
                }
            }
        }
    }

    private void notifyISpouts(){
        List<ISpout> iSpouts = getISpouts();
        for (ISpout iSpout : iSpouts){
            int parallel = 1;
            Parallel annotation = iSpout.getClass().getAnnotation(Parallel.class);
            if (annotation!=null){
                parallel = annotation.parallel();
            }
            for(int i=0;i<parallel;i++){
                SpoutTask spoutTask = new SpoutTask(iSpout,monitor);
                spoutTasks.add(spoutTask);
                new Thread(spoutTask).start();
            }
        }
    }

    public List<IBolt> getIBolts() {
        if(iBolts!=null&&iBolts.size()>0){
            return iBolts;
        }
        return new ArrayList<>();
    }

    public List<ISpout> getISpouts() {
        if (iSpouts!=null&&iSpouts.size()>0){

            return iSpouts;
        }
        return new ArrayList<>();
    }

    public List<SpoutTask> getSpoutTasks() {
        return spoutTasks;
    }
}
