/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.config;

import java.util.ArrayList;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.prefs.Preferences;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;
import org.apache.log4j.Logger;

/**
 *
 * @author root
 */
public class ZConfigReader {

    private static ZConfigReader cfgIns_ = null;
    public Preferences prefs_ = null;
    private static final Logger logger = Logger.getLogger(ZConfigReader.class);
    
    private List<String> lstRedis = new ArrayList<String>();
    private int jettyPortListen = 0;
    private int jettyMinThreads = 50;
    private int jettyMaxThreads = 100;
    private int jettyAcceptors = 3;
    private int jettyLowResourcesConnections = 20000;
    private int jettyLowResourcesMaxIdleTime = 5000;
    
    public List<String> getListRedis() {
        return lstRedis;
    }

    public int getJettyPortListen() {
        return jettyPortListen;
    }

    public int getJettyMinThreads() {
        return jettyMinThreads;
    }

    public int getJettyMaxThreads() {
        return jettyMaxThreads;
    }

    public int getJettyAcceptors() {
        return jettyAcceptors;
    }

    public int getJettyLowResourcesConnections() {
        return jettyLowResourcesConnections;
    }

    public int getJettyLowResourcesMaxIdleTime() {
        return jettyLowResourcesMaxIdleTime;
    }
    
    public static ZConfigReader getConfigReaderIns() {
        try {
            if (cfgIns_ == null) {
                cfgIns_ = new ZConfigReader("config.ini");
            }
            return cfgIns_;
        } catch (Exception ex) {
            logger.error("Create install config reader has exception: " + ex.getMessage());
            return null;
        }
    }

    private ZConfigReader(String file_cfg) throws IOException {
        prefs_ = new IniPreferences(new Ini(new File(file_cfg)));
        ReadConfig();
    }

    public void ReadConfig() {
        //[ge_mq]
        //prefix_response_channel=s2g_
        //prefixChannelResponse = getString("ge_mq", "prefix_response_channel", "s2g_");
        
        //[ge_mq]
        //mq_produce_list=1,2,3,4,5,6
        //lstSIDProduce = getListInteger("ge_mq", "mq_produce_list");
        
        //[ge_mq]
        //num_mq=10
        //Long num_mq = getLong("ge_mq", "num_mq", 0L);
        
        //config server http
        jettyPortListen = getInteger("config", "listenPort", 0);
        jettyMinThreads = getInteger("config", "minThreads", 50);
        jettyMaxThreads = getInteger("config", "maxThreads", 100);
        jettyAcceptors  = getInteger("config", "acceptors", 3);
        jettyLowResourcesConnections = getInteger("config", "lowResourcesConnections", 20000);
        jettyLowResourcesMaxIdleTime = getInteger("config", "lowResourcesMaxIdleTime", 5000);
        
        //config redis
        Long num_redis = getLong("redis", "num_redis", 0L);
        if (num_redis == 0L) {
            logger.error("Cannot get any redis server from config => stop service ...");
            return;
        }
        
        String uri = "";
        String rds_section = "";
        for (int i = 0; i < num_redis; i++) {
            rds_section = "redis_" + String.valueOf(i + 1);
            uri = getString(rds_section, "uri", "");
            if (uri.isEmpty()) {
                continue;
            }

            lstRedis.add(uri);
            logger.info("Add redis " + uri + " to list cluster");
        }
    }

    public String getString(String section, String key, String def) {
        return prefs_.node(section).get(key, def);
    }

    private Long getLong(String section, String key, Long def) {
        String sInt = prefs_.node(section).get(key, null);
        if (sInt == null || sInt.isEmpty()) {
            return def;
        }

        Long res = tryParseLong(sInt);
        if (res == null) {
            return def;
        }

        return res;
    }
    
    private Integer getInteger(String section, String key, Integer def) {
        String sInt = prefs_.node(section).get(key, null);
        if (sInt == null || sInt.isEmpty()) {
            return def;
        }

        Integer res = tryParseInteger(sInt);
        if (res == null) {
            return def;
        }

        return res;
    }

    //http://www.java2s.com/Code/Java/Regular-Expressions/Extractasubstringbymatchingaregularexpression.htm
    private List<Integer> getListInteger(String section, String key) {
        List<Integer> res = new ArrayList<Integer>();

        String iniStr = getString(section, key, "");
        if (iniStr == null || iniStr.isEmpty()) {
            return res;
        }

        Pattern pattent = Pattern.compile("[0-9]+");
        Matcher mat = pattent.matcher(iniStr);
        Integer iter = 0;
        while (mat.find()) {
            iter = tryParseInteger(mat.group());
            if (iter != null) {
                res.add(iter);
            }
        }

        return res;
    }

    private Long tryParseLong(String text) {
        try {
            if (text == null) {
                return null;
            }
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private Integer tryParseInteger(String text) {
        try {
            if (text == null) {
                return null;
            }
            return Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
