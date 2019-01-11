package com.iquantex.flowhandler.helper;

public class LatencyHelper {

    private final static ThreadLocal<Long> _threadLocal = new ThreadLocal<>();

    public static void startTiming() {
        _threadLocal.set(System.currentTimeMillis());
    }

    public static Long stopTiming() {
        Long startTime = _threadLocal.get();
        if (null == startTime) {
            startTime = System.currentTimeMillis();
        }
        _threadLocal.remove();
        return System.currentTimeMillis() - startTime;
    }
}
