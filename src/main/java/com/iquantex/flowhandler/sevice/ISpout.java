package com.iquantex.flowhandler.sevice;


import com.iquantex.flowhandler.bean.ReportMsg;

public interface ISpout extends IComponent {

    void execute(Emitor emitor);

    void ack(ReportMsg reportMsg);

    void fail(ReportMsg reportMsg);

}
