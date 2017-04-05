/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.redis.util;

import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.cluster.ClusterClientOptions;
import com.lambdaworks.redis.cluster.RedisClusterClient;
import com.lambdaworks.redis.cluster.api.sync.RedisAdvancedClusterCommands;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import http.config.ZConfigReader;

/**
 *
 * @author root
 */
public class RedisUtil {
    //private static RedisClient redisClient = null;
    private static RedisClusterClient redisClusterClient = null;
    private static RedisAdvancedClusterCommands<String, String> redisStringCommand = null;
    private static RedisAdvancedClusterCommands<String, byte[]> redisByteCommand = null;
    
    private static void createRdsIns()
    {
/*
        RedisClusterClient clusterClient = new RedisClusterClient(RedisURI.create("redis://localhost:8000"));
        RedisAdvancedClusterConnection<String, String> cluster = clusterClient.connectCluster();
*/
/*
        redisURIs.add(RedisURI.create("redis://127.0.0.1:8000"));
        redisURIs.add(RedisURI.create("redis://127.0.0.1:8001"));
        redisURIs.add(RedisURI.create("redis://127.0.0.1:8002"));
        redisURIs.add(RedisURI.create("redis://127.0.0.1:8003"));
        redisURIs.add(RedisURI.create("redis://127.0.0.1:8004"));
        redisURIs.add(RedisURI.create("redis://127.0.0.1:8005"));
*/
        
        List<RedisURI> redisURIs = new ArrayList<RedisURI>();
        List<String> lstRedis = ZConfigReader.getConfigReaderIns().getListRedis();
        for(int i = 0; i < lstRedis.size(); i++)
        {
            redisURIs.add(RedisURI.create(lstRedis.get(i)));
        }
        
        redisClusterClient = RedisClusterClient.create(redisURIs);
        redisClusterClient.setOptions(new ClusterClientOptions.Builder()
                .refreshClusterView(true)
                .refreshPeriod(1, TimeUnit.MINUTES)
                .build());
        
        //System.out.println(redisClient.getPartitions().toString());
        //StatefulRedisClusterConnection<String, String> connection = redisClusterClient.connect();
        //connection.sync()
        
        //connection.close();
        //redisClient.shutdown();
    }

    //Instances Redis
    public static RedisClusterClient getRdsIns() {
        if (redisClusterClient == null) {
            createRdsIns();
        }
        return redisClusterClient;
    }

    public static RedisAdvancedClusterCommands<String, byte[]> getRdsByteCmdIns() {
        if (redisByteCommand == null) {
            redisByteCommand = getRdsIns().connect(new ByteCodec()).sync();
        }
        return redisByteCommand;
    }

    public static RedisAdvancedClusterCommands<String, String> getRdsStringCmdIns() {
        if (redisStringCommand == null) {
            redisStringCommand = getRdsIns().connect().sync();
        }
        return redisStringCommand;
    }
    
    public static void setStringValue(String key, String value)
    {
        try
        {
            if ((key == null) || (value == null))
                return;
            getRdsStringCmdIns().set(key, value);
        }
        catch(Exception ex){}
    }
    
    public static void setByteValue(String key, byte[] value)
    {
        try
        {
            if ((key == null) || (value == null))
                return;
            getRdsByteCmdIns().set(key, value);
        }
        catch(Exception ex){}
    }
    
    public static String getHashStringValue(String hash, String key)
    {
        try
        {
            String s = getRdsStringCmdIns().hget(hash, key);
            return s;
        }
        catch(Exception e)
        {
            return null;
        }
    }
    
    public static Long getHashLongValue(String hash, String key)
    {
        try
        {
            String s = getRdsStringCmdIns().hget(hash, key);
            return tryParseLong(s);
        }
        catch(Exception e)
        {
            return null;
        }
    }
    
    public static Long isExistKey(String... key)
    {
        return getRdsStringCmdIns().exists(key);
    }
    
    public static Integer tryParseInt(String text) {
        try {
            if(text == null)
                return null;
            return Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public static Long tryParseLong(String text) {
        try {
            if(text == null)
                return null;
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
    
}
