package com.iquantex.flowhandler.sevice;


import com.iquantex.flowhandler.bean.ReportMsg;
import com.iquantex.flowhandler.bean.Tuple;
import com.iquantex.flowhandler.helper.LatencyHelper;
import com.iquantex.flowhandler.monitor.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.*;

public class BoltTask {
    private final static Logger LOG = LoggerFactory.getLogger(BoltTask.class);

    private IBolt iBolt;
    private Monitor monitor;
    private Queue<Tuple> queue = new ConcurrentLinkedQueue<>();
    private int parallel;

    public BoltTask(IBolt iBolt, Monitor monitor, int parallel){
        this.iBolt = iBolt;
        this.monitor = monitor;
        this.parallel = parallel;
    }

    public void putMsg(Tuple tuple){
        queue.offer(tuple);
    }

    public void execute(Tuple msg){
        ReportMsg reportMsg = new ReportMsg();
        reportMsg.setSource(msg.getSourceComponent());
        reportMsg.setCurrent(iBolt.getClass().getSimpleName());
        reportMsg.setQueueSize(queue.size());
        try {
            LatencyHelper.startTiming();
            Emitor emitor = new Emitor(iBolt,msg.getSourceComponent());
            iBolt.execute(msg,emitor);
            //成功
            reportMsg.setStatus(ReportMsg.SUCESS);
        }catch (Exception e){
            LOG.error(e.getMessage());
            e.printStackTrace();

            //失败
            reportMsg.setStatus(ReportMsg.FAILURE);
        }finally {
            Long timeCost = LatencyHelper.stopTiming();
            reportMsg.setTime(timeCost);
            monitor.report(reportMsg);
        }
    }

    public void start(){
        int poolSize = getParallel();
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        for (int i = 0;i<poolSize;i++){
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    while (true){
                    Tuple msg = queue.poll();
                    if (msg!=null){
                        execute(msg);
                    }
                }
                }
            };
            executorService.execute(run);
        }
    }

    public IBolt getFlowNode() {
        return iBolt;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

}
