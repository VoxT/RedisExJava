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
import java.util.HashSet;
import java.util.Set;

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
    private static RedisClusterClient getRdsIns() {
        if (redisClusterClient == null) {
            createRdsIns();
        }
        return redisClusterClient;
    }

    private static RedisAdvancedClusterCommands<String, byte[]> getRdsByteCmdIns() {
        if (redisByteCommand == null) {
            redisByteCommand = getRdsIns().connect(new ByteCodec()).sync();
        }
        return redisByteCommand;
    }

    private static RedisAdvancedClusterCommands<String, String> getRdsStringCmdIns() {
        if (redisStringCommand == null) {
            redisStringCommand = getRdsIns().connect().sync();
        }
        return redisStringCommand;
    }
    
    /**
     * 
     * @param strValue
     * @return true if string is valid,
     *         false if string is invalid.
     */
    private static boolean isValidString(String... strValue)
    {
        if (strValue == null)
            return false;
        for (String str : strValue) {
            if ((str == null) || str.isEmpty())
                return false;
        }
        
        return true;
    }
    
    /**
     * 
     * @param key
     * @return The number of keys existing among the ones specified as arguments. 
     *         null if failed.
     */
    public static Long isExistKey(String... key)
    {        
        if (!isValidString(key))
            return null;
        
        try
        {            
            return getRdsStringCmdIns().exists(key);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @return The number of keys that were removed.
     *         null if failed.
     */
    public static Long deleteKey(String... key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            return getRdsStringCmdIns().del(key);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @return the value of key after the increment.
     *         null if failed.
     */
    public static Long incr(String key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            return getRdsStringCmdIns().incr(key);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**    
     * 
     * @param key
     * @param value
     * @return true if succeed.
     *         false if failed.
     */
    public static boolean setStringValue(String key, String value)
    {
        if (!isValidString(key, value))
            return false;
        
        try
        {            
            if (getRdsStringCmdIns().set(key, value).compareToIgnoreCase("OK") != 0)
                return false;
        }
        catch(Exception ex)
        {
            return false;
        }
        
        return true;
    }
    
    public static Long getLongvalue(String key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            return tryParseLong(getRdsStringCmdIns().get(key));
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param field
     * @param value
     * @return true if field is a new field in the hash and value was set.
     *         false if field already exists in the hash and the value was updated.
     *         null if failed.
     */
    public static Boolean hSet(String key, String field, String value)
    {
        if (!isValidString(key, field, value))
            return null;
        
        try
        {
            return getRdsStringCmdIns().hset(key, field, value);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param field
     * @return the value associated with field, 
     *         or null when field is not present in the hash or key does not exist.
     */
    public static String hGetStringValue(String key, String field)
    {
        if (!isValidString(key, field))
            return null;
        
        try
        {
            String s = getRdsStringCmdIns().hget(key, field);
            return s;
        }
        catch(Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param field
     * @return the value associated with field, 
     *         or null when field is not present in the hash or key does not exist.
     */
    public static Long hGetLongValue(String key, String field)
    {
        if (!isValidString(key, field))
            return null;
        
        try
        {
            String s = getRdsStringCmdIns().hget(key, field);
            return tryParseLong(s);
        }
        catch(Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param member
     * @return the number of elements that were added to the set, 
     *         not including all the elements already present into the set.
     *          null if failed.
     */
    public static Long sAdd(String key, String... member)
    {        
        if (!isValidString(key))
            return null;
        if (!isValidString(member))
            return null;
        
        try
        {
            return getRdsStringCmdIns().sadd(key, member);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @return the cardinality (number of elements) of the set, 
     *         or 0 if key does not exist.
     *         null if failed.
     */
    public static Long sCard(String key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            return getRdsStringCmdIns().scard(key);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param member
     * @return true if the element is a member of the set.
     *         false if the element is not a member of the set, or if key does not exist.\
     *         null if failed.
     */
    public static Boolean sIsMember(String key, String member)
    {
        if (!isValidString(key, member))
            return null;
        
        try 
        {
            return getRdsStringCmdIns().sismember(key, member);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @return all elements of the set.
     *         null if failed.
     */
    public static Set<String> sMembersStringValue(String key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            return getRdsStringCmdIns().smembers(key);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    
    /**
     * 
     * @param key
     * @return all elements of the set.
     *         null if failed.
     */
    public static Set<Long> sMembersLongValue(String key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            Set<String> stringMembers = getRdsStringCmdIns().smembers(key);
            Set<Long> longMembers = new HashSet<>();
            Long value;
            for (String member : stringMembers)
            {
                value = tryParseLong(member);
                if (null == value)
                    continue;
                
                longMembers.add(value);
            }
            return longMembers;
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param member
     * @return  the number of members that were removed from the set,
     *          not including non existing members.
     *          null if failed.
     */
    public static Long sRem(String key, String... member)
    {
        if (!isValidString(key))
            return null;
        if (!isValidString(member))
            return null;
        
        try
        {
            return getRdsStringCmdIns().srem(key, member);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param score
     * @param member
     * @return The number of elements added to the sorted sets,
     *         not including elements already existing for which the score was updated.
     *         null if failed.
     */
    public  static Long zAdd(String key, double score, String member)
    {
        if (!isValidString(key, member))
            return null;
        
        try 
        {
            return getRdsStringCmdIns().zadd(key, score, member);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @return the cardinality (number of elements) of the sorted set, 
     *         or 0 if key does not exist.
     *         null if failed.
     */
    public static Long zCard(String key)
    {
        if (!isValidString(key))
            return null;
        
        try
        {
            return getRdsStringCmdIns().zcard(key);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param start
     * @param stop
     * @return list of elements in the specified range.
     *         null if failed.
     */
    public static List<String> zRangeStringValue(String key, long start, long stop)
    {
        if (!isValidString(key))
            return null;
        
        try 
        {
            return getRdsStringCmdIns().zrange(key, start, stop);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param start
     * @param stop
     * @return list of elements in the specified range.
     *         null if failed.
     */
    public static List<Long> zRangeLongValue(String key, long start, long stop)
    {
        if (!isValidString(key))
            return null;
        
        try 
        {
            List<String> stringMembers = getRdsStringCmdIns().zrange(key, start, stop);
            List<Long> longMembers = new ArrayList<>();
            Long value;
            for (String member : stringMembers)
            {
                value = tryParseLong(member);
                if (null == value)
                    continue;
                
                longMembers.add(value);
            }
            return longMembers;
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param member
     * @return The number of members removed from the sorted set, 
     *         not including non existing members.
     *         null if failed.
     */
    public static Long zRem(String key, String... member)
    {
        if (!isValidString(key))
            return null;
        if (!isValidString(member))
            return null;
        
        try
        {
            return getRdsStringCmdIns().zrem(key, member);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param member
     * @return the score of member (a double precision floating point number).
     *         null if failed.
     */
    public static Double zScore(String key, String member)
    {
        if (!isValidString(key, member))
            return null;
        
        try
        {
            return getRdsStringCmdIns().zscore(key, member);
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    /**
     * 
     * @param key
     * @param member
     * @return the rank of member if member exists in the sorted set,
     *         null if member does not exist in the sorted set or key does not exist.
     */
    public static Long zRank(String key, String member)
    {
        if (!isValidString(key, member))
            return null;
        
        try
        {
           return getRdsStringCmdIns().zrank(key, member);
        }
        catch (Exception ex)
        {
            return null;
        }
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
