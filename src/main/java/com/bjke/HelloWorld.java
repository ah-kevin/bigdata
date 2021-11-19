package com.bjke;

import org.apache.log4j.Logger;

public class HelloWorld {
    public static void main(String[] args) {
        Logger logger = Logger.getLogger(HelloWorld.class);
        logger.debug("debug");
        logger.info("info");
        logger.warn("warn");
        logger.error("error");
        logger.fatal("fatal");
    }
}
