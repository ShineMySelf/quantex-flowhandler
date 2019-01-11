package com.iquantex.flowhandler.sevice;


import com.iquantex.flowhandler.bean.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class TupleMarket {
    private static Map<String, BlockingQueue> _mQueue = new HashMap<>();

    public static void putMsg(String topic, Tuple tuple) {
        BlockingQueue queue = getQueue(topic);
        try {
            queue.put(tuple);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Tuple pollMsg(String topic) {
        BlockingQueue queue = getQueue(topic);
        return (Tuple) queue.poll();
    }

    private static BlockingQueue getQueue(String topic) {
        BlockingQueue queue = _mQueue.get(topic);
        if (null == queue) {
            synchronized (TupleMarket.class) {
                if (null == queue) {
                    queue = new LinkedBlockingQueue();
                    _mQueue.put(topic, queue);
                }
            }
        }
        return queue;
    }
}
