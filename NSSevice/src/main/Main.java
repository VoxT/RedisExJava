package main;

import com.vng.jcore.common.LogUtil;
import httpserver.WebServer;
import java.io.File;
import org.apache.log4j.Logger;

import http.config.ZConfigReader;



public class Main {

    private static Logger logger = Logger.getLogger(Main.class);;
    
    public static void main(String[] args) throws Exception {
        try {
            LogUtil.init();
            ZConfigReader.getConfigReaderIns();

            String pidFile = System.getProperty("pidfile");
            if (pidFile != null) {
                new File(pidFile).deleteOnExit();
            }
            
            WebServer webserver = new WebServer();
            webserver.start();
            
        } catch (Throwable e) {
            logger.error("Exception at start up : " + e.getMessage(), e);
            System.exit(3);
        }
    }
}
