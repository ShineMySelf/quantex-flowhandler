package com.iquantex.flowhandler.sevice;

import com.iquantex.flowhandler.bean.ReportMsg;
import com.iquantex.flowhandler.monitor.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpoutTask implements Runnable{

    private final static Logger LOG = LoggerFactory.getLogger(SpoutTask.class);

    private ISpout iSpout;

    private Monitor monitor;

    private boolean status = false;

    public SpoutTask(ISpout iSpout, Monitor monitor){
        this.iSpout = iSpout;
        this.monitor = monitor;
    }

    @Override
    public void run() {
        while (true){
            synchronized (this) {
                while (status) {
                    try {
                        System.out.println(Thread.currentThread().getName() + ":线程阻塞,源节点：" + this.iSpout.getClass().getSimpleName());
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                ReportMsg reportMsg = new ReportMsg();
                reportMsg.setSource(this.iSpout.getClass().getSimpleName());

                try {
                    String sourceComponent = this.iSpout.getClass().getSimpleName();
                    Emitor emitor = new Emitor(iSpout, sourceComponent);
                    iSpout.execute(emitor);
                    reportMsg.setStatus(ReportMsg.SUCESS);
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                    e.printStackTrace();
                    reportMsg.setStatus(ReportMsg.FAILURE);
                } finally {
                    monitor.report(reportMsg);
                }
            }
        }
    }

    public void notifyTask(){
        synchronized (this){
            this.status = false;
            this.notify();
        }
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public ISpout getiSpout() {
        return iSpout;
    }
}
