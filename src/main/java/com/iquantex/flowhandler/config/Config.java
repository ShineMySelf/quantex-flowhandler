package com.iquantex.flowhandler.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class Config {
    private final static Logger LOG = LoggerFactory.getLogger(Config.class);

    public static boolean FLOW_ENABLED = false;

    public static int FLOW_QUEUE_SIZE = Integer.MAX_VALUE;

    static{
        loadProperty();
    }

    public static void loadProperty() {
        InputStream in = Config.class.getResourceAsStream("/quantex-flowhandler.properties");
        Properties pro = new Properties();
        try {
            pro.load(in);
        } catch (Exception e) {
            LOG.warn("load quantex-flowhandler.properties fail ...");
        }
        FLOW_QUEUE_SIZE = Integer.valueOf(pro.getProperty("flow.queue.size",String.valueOf(Integer.MAX_VALUE)));
        FLOW_ENABLED = Boolean.valueOf(pro.getProperty("flow.enabled",String.valueOf(false)));
    }

}
