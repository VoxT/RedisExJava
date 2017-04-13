/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.api.handler;

import http.api.define.ZAPIDefine;
import http.api.define.JSMessage;
import com.google.protobuf.ByteString;
import http.api.utils.CommonUtils;
import http.api.utils.GsonUtils;
import http.redis.util.RedisUtil;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;

/**
 *
 * @author root
 */
public class ZAPIMsgHandler extends BaseApiHandler {
    private static final String RDS_NS_MSG_INFO_FIELD_DATA                 = "data";
    private static final String RDS_NS_MSG_INFO_FIELD_SENDER_ID            = "senderid";
    private static final String RDS_NS_MSG_INFO_FILED_RESULT               = "result";
    private static final String RDS_NS_MSG_INFO_FIELD_REQUEST_TIME         = "reqtime";
    private static final String RDS_NS_MSG_INFO_FIELD_SEND_TIME            = "sendtime";

    private static final String RDS_NS_USERS                               = "ns:list_user";
    private static final String RDS_NS_SENDERS                             = "ns:list_sender";
    
    private static final int SIZE_PER_GET                                  = 3;
    private static final int MAX_TIME_EXPIRE_KEY                           = 172800; // seconds (48h)

    private static final ZAPIMsgHandler instance = new ZAPIMsgHandler();

    public static ZAPIMsgHandler getInstance() {
        return instance;
    }
    
    @Override
    public String doAction(HttpServletRequest req) {
        try {
            if (req.getMethod().compareToIgnoreCase("POST") != 0)
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_WRONG_METHOD, 
                                                "Wrong method. Must be POST");
            
            int contentLength = req.getContentLength();
            if (contentLength > ZAPIDefine.MAX_REQ_BUFFER_SIZE)
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_REQ_SIZE_TOO_BIG, 
                                                "Request size too big");
            
            // Read request data
            InputStream inStream = req.getInputStream();
            ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();

            byte[] recvBuf = new byte[ZAPIDefine.MAX_REQ_BUFFER_SIZE];
            int readBytes = 0, countBytes = 0;

            while (countBytes < contentLength) {
                readBytes = inStream.read(recvBuf);
                baOutStream.write(recvBuf, 0, readBytes);
                countBytes += readBytes;
            }
            if (countBytes != contentLength)
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_READ_REQ_FAILED, 
                                                "Read request failed");

            // parse json
            String jsonString = new String(baOutStream.toByteArray(), Charset.forName("UTF-8"));
            JSMessage jsMsg = GsonUtils.fromJsonString(jsonString, JSMessage.class);
            if (jsMsg == null)
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_PARSE_FAIL, 
                                                "parse json failed.");
            if (!validateLongValue(jsMsg.userId, jsMsg.senderId))
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_INVALID_ID, 
                                                "Invalid id uId=" + Long.toString(jsMsg.userId) 
                                                + " sId=" + Long.toString(jsMsg.senderId));
            
            //do someting
            System.err.println("sid=" + jsMsg.senderId + " uid=" + jsMsg.userId 
                    + " data=" + jsMsg.data);
            
            long reqTime = System.currentTimeMillis();
            Random ran = new Random();
            long sendTime = System.currentTimeMillis() + ran.nextInt(100) + 10;
            
            if (!saveInfo(jsMsg, reqTime, sendTime, proccessData(jsMsg)))
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_SAVE_MSG_FAIL, 
                                                "save msg failed");
           
            //return json
            return CommonUtils.handleResult(ZAPIDefine.API_RESULT_SUCCESS, 
                                            ZAPIDefine.API_RES_SUCCESS, 
                                            "success");

        } catch (Exception ex) {
            return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                            ZAPIDefine.API_RES_SAVE_MSG_FAIL, 
                                            "opp! something went wrong " + ex.getMessage());
        }
    }
    
    private boolean validateLongValue(long... value)
    {
        for (long val : value)
        {
            if (val < 0)
                return false;
        }
        
        return true;
    }
    
    private boolean proccessData(JSMessage jsMsg)
    {
        if (jsMsg == null)
            return false;
        
        return ((System.currentTimeMillis()%2) == 1);
    }
    
    private boolean saveInfo(JSMessage jsMsg, long reqTime, long sendTime, boolean result)
    {
        if (jsMsg == null)
            return false;
        if (!validateLongValue(jsMsg.userId, jsMsg.senderId, reqTime, sendTime))
            return false;
        
        // HASH msg info
        Long msgId = saveMsgInfo(jsMsg, reqTime, sendTime, result);
        if (msgId == null)
            return false;
        
        // ZSET msgid of userid and senderid
        String listMsgIdKey = getKeyListMsgOfSIdAndUId(jsMsg.userId, jsMsg.senderId);
        if (listMsgIdKey == null)
            return false;
        if (RedisUtil.zAdd(listMsgIdKey, reqTime, Long.toUnsignedString(msgId)) == null)
            return false;
        
        // SET users by senderid
        String listUIdBySIdKey = getKeyListUserBySenderID(jsMsg.senderId);
        if (listUIdBySIdKey == null)
            return false;
        if (RedisUtil.sAdd(listUIdBySIdKey, Long.toUnsignedString(jsMsg.userId)) == null)
            return false;
                
        // ZSET users
        if (RedisUtil.zAdd(RDS_NS_USERS, jsMsg.userId, Long.toUnsignedString(jsMsg.userId)) == null)
            return false;
        // ZSET senders
        if (RedisUtil.zAdd(RDS_NS_SENDERS, jsMsg.senderId, Long.toUnsignedString(jsMsg.senderId)) == null)
            return false;
        
        return true;
    }
    
    private String getKeyUserMsgCounter(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        return ("ns:user:" + Long.toUnsignedString(userId) + ":msg_counter");
    }
    
    private String getKeyMsgInfo(long userId, long msgId)
    {
        if (!validateLongValue(userId, msgId))
            return null;
        
        return ("ns:msg:" + Long.toUnsignedString(userId) + ":" + Long.toUnsignedString(msgId) + ":info");
    }
    
    private String getKeyListUserBySenderID(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        return ("ns:list_userid:" + Long.toUnsignedString(senderId));
    }
    
    private String getKeyListMsgOfSIdAndUId(long userId, long senderId)
    {
        if (!validateLongValue(userId, senderId))
            return null;
        
        return ("ns:list_msgid:" + Long.toUnsignedString(senderId) + ":" + Long.toUnsignedString(userId));
    }

    private Long saveMsgInfo(JSMessage jsMsg, long reqTime, long sendTime, boolean result) 
    {
        if (jsMsg == null)
            return null;
        if (!validateLongValue(jsMsg.userId, jsMsg.senderId, reqTime, sendTime))
            return null;
        if (reqTime > sendTime)
            return null;
        
        // Get msgid
        String msgCounterKey = getKeyUserMsgCounter(jsMsg.userId);
        if (msgCounterKey == null)
            return null;
        Long msgId = RedisUtil.incr(msgCounterKey);
        if (msgId == null)
            return null;
        
        //set hash msg info
        String msgInfoKey = getKeyMsgInfo(jsMsg.userId, msgId);
        if (msgInfoKey == null)
            return null;
        if (RedisUtil.hSet(msgInfoKey, RDS_NS_MSG_INFO_FIELD_DATA, jsMsg.data) == null)
            return null;
        if (RedisUtil.hSet(msgInfoKey, RDS_NS_MSG_INFO_FIELD_REQUEST_TIME, Long.toUnsignedString(reqTime)) == null)
            return null;
        if (RedisUtil.hSet(msgInfoKey, RDS_NS_MSG_INFO_FIELD_SEND_TIME, Long.toUnsignedString(sendTime)) == null)
            return null;
        if (RedisUtil.hSet(msgInfoKey, RDS_NS_MSG_INFO_FILED_RESULT, result ? "1":"0") == null)
            return null;
        if (RedisUtil.hSet(msgInfoKey, RDS_NS_MSG_INFO_FIELD_SENDER_ID, Long.toUnsignedString(jsMsg.senderId)) == null)
            return null;
        
        return msgId;
    }

    private Long getUserMsgCounter(long userId) {
        if (!validateLongValue(userId))
            return null;
        
        String msgCounterKey = getKeyUserMsgCounter(userId);
        if (msgCounterKey == null)
            return null;
        
        Long msgId = RedisUtil.getLongvalue(msgCounterKey);
        if (msgId == null)
            return null;
        
        return msgId;
    }

    public List<Long> getListUser(long start, long stop)
    {
        return RedisUtil.zRangeLongValue(RDS_NS_USERS, start, stop);
    }
    
    public List<Long> getListSender(long start, long stop)
    {
        return RedisUtil.zRangeLongValue(RDS_NS_SENDERS, start, stop);
    }
    
    public Long getTotalUser()
    {
        return RedisUtil.zCard(RDS_NS_USERS);
    }
        
    public Long getTotalSender()
    {
        return RedisUtil.zCard(RDS_NS_SENDERS);
    }
    
    public Long getTotalRequest()
    {
        Long totalUser = getTotalUser();
        if (totalUser == null)
            return null;
        
        long selectSize = 0, selectCount = 0, totalReq = 0;
        List<Long> listUser;
        while (selectCount < totalUser)
        {
            selectSize = Math.min(SIZE_PER_GET, totalUser - selectCount);
            listUser = getListUser(selectCount, selectCount + selectSize - 1);
            if (listUser == null)
                return null;
            
            Long msgCounter;
            for (int i = 0; i < listUser.size(); i++)
            {
                msgCounter = getUserMsgCounter(listUser.get(i));
                if (msgCounter == null)
                    return null;
                
                totalReq += msgCounter;
            }
            selectCount += listUser.size();
        }
        
        return totalReq;
    }
    
    private Boolean getMsgResult(long userId, long msgId)
    {
        if (!validateLongValue(userId, msgId))
            return null;
        
        String msgKey = getKeyMsgInfo(userId, msgId);
        if (msgKey == null)
            return null;

        Long result = RedisUtil.hGetLongValue(msgKey, RDS_NS_MSG_INFO_FILED_RESULT);
        if (result == null)
            return null;
        
        return (result != 0);
    }
    
    public Long getTotalMsgByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        return getUserMsgCounter(userId) ;
    }
    
    public Long getTotalSucceedByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
                
        long totalSucceed = 0;
        Boolean result;
        for (long msgId = 1; msgId <= msgCounter; msgId++)
        {
            result = getMsgResult(userId, msgId);
            if(result == null)
                return null;
            if (result)
                totalSucceed++;
        }
        
        return totalSucceed;
    }
    
    public Long getTotalFailedByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
                
        long totalFailed = 0;
        Boolean result;
        for (long msgId = 1; msgId <= msgCounter; msgId++)
        {
            result = getMsgResult(userId, msgId);
            if (result == null)
                return null;
            if (!result)
                totalFailed++;
        }
        
        return totalFailed;
    }
    
    public Long getTotalMsgBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> setUser = getListUserBySenderID(senderId);
        if (setUser == null)
            return null;
        if (setUser.isEmpty())
            return null;
        
        long totalMsg = 0;
        Long listMsgSize;
        String strlistMsgKey;
        for (Long userId : setUser)
        {
            strlistMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (strlistMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(strlistMsgKey);
            if (listMsgSize == null)
                return null;
            
            totalMsg += listMsgSize;
        }
        
        return totalMsg;
    }
    
    public Long getTotalSucceedBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> setUser = getListUserBySenderID(senderId);
        if (setUser == null)
            return null;
        if (setUser.isEmpty())
            return null;
        
        long totalSucceed = 0;
        Long listMsgSize;
        String strlistMsgKey;
        for (Long userId : setUser)
        {
            strlistMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (strlistMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(strlistMsgKey);
            if (listMsgSize == null)
                return null;
            
            long selectSize = 0, selectCount = 0;
            List<Long> listMsg;
            Boolean result;
            while (selectCount < listMsgSize)
            {
                selectSize = Math.min(SIZE_PER_GET, listMsgSize - selectCount);
                listMsg = RedisUtil.zRangeLongValue(strlistMsgKey, selectCount, selectCount + selectSize - 1);
                if (listMsg == null)
                    return null;
                
                for (int i = 0; i < listMsg.size(); i++)
                {
                    result = getMsgResult(userId, listMsg.get(i));
                    if (result == null)
                        return null;
                    if (result)
                        totalSucceed++;
                }
                
                selectCount += selectSize;
            }
        }
        
        return totalSucceed;
    }
    
    public Long getTotalFailedBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> listUser = getListUserBySenderID(senderId);
        if (listUser == null)
            return null;
        if (listUser.isEmpty())
            return null;
        
        long totalFailed = 0;
        Long listMsgSize;
        String strlistMsgKey;
        for (Long userId : listUser)
        {
            strlistMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (strlistMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(strlistMsgKey);
            if (listMsgSize == null)
                return null;
            
            long selectSize = 0, selectCount = 0;
            List<Long> listMsg;
            Boolean result;
            while (selectCount < listMsgSize)
            {
                selectSize = Math.min(SIZE_PER_GET, listMsgSize - selectCount);
                listMsg = RedisUtil.zRangeLongValue(strlistMsgKey, selectCount, selectCount + selectSize - 1);
                if (listMsg == null)
                    return null;
                
                for (int i = 0; i < listMsg.size(); i++)
                {
                    result = getMsgResult(userId, listMsg.get(i));
                    if (result == null)
                        return null;
                    if (!result)
                        totalFailed++;
                }
                
                selectCount += selectSize;
            }
        }
        
        return totalFailed;
    }
    
    public Long getMsgProcessedTime(long userId, long msgId)
    {
        if (!validateLongValue(userId, msgId))
            return null;
        
        String msgKey = getKeyMsgInfo(userId, msgId);
        if (msgKey == null)
            return null;

        Long reqTime = RedisUtil.hGetLongValue(msgKey, RDS_NS_MSG_INFO_FIELD_REQUEST_TIME);
        if (reqTime == null)
            return null;
        
        Long sendTime = RedisUtil.hGetLongValue(msgKey, RDS_NS_MSG_INFO_FIELD_SEND_TIME);
        if (sendTime == null)
            return null;
        
        return (sendTime - reqTime);
    }
    
    public Long getMinProcessedTimeByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
                
        long minPTime = Long.MAX_VALUE;
        Long pTime;
        for (long msgId = 1; msgId <= msgCounter; msgId++ )
        {
            pTime = getMsgProcessedTime(userId, msgId);
            if (pTime == null)
                return null;
            
            minPTime = Math.min(minPTime, pTime);
        }
        
        return minPTime;
    }
    
    public Long getMaxProcessedTimeByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
                
        long maxPTime = 0;
        Long pTime;
        for (long msgId = 1; msgId <= msgCounter; msgId++ )
        {
            pTime = getMsgProcessedTime(userId, msgId);
            if (pTime == null)
                return null;
            
            maxPTime = Math.max(maxPTime, pTime);
        }
        
        return maxPTime;
    }
    
    public Double getAverageProcessedTimeByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
                
        long sumPTime = 0;
        Long pTime;
        for (long msgId = 1; msgId <= msgCounter; msgId++ )
        {
            pTime = getMsgProcessedTime(userId, msgId);
            if (pTime == null)
                return null;
            
            sumPTime += pTime;
        }
        
        if (msgCounter == 0)
            return null;
        
        return ((double) sumPTime / (double) msgCounter);
    }    
    
    public Long getMinProcessedTimeBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> listUser = getListUserBySenderID(senderId);
        if (listUser == null)
            return null;
        if (listUser.isEmpty())
            return null;
        
        long minPTime = Long.MAX_VALUE;
        Long listMsgSize;
        String strlistMsgKey;
        for (Long userId : listUser)
        {
            strlistMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (strlistMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(strlistMsgKey);
            if (listMsgSize == null)
                return null;
            
            long selectSize = 0, selectCount = 0;
            List<Long> listMsg;
            Long pTime;
            while (selectCount < listMsgSize)
            {
                selectSize = Math.min(SIZE_PER_GET, listMsgSize - selectCount);
                listMsg = RedisUtil.zRangeLongValue(strlistMsgKey, selectCount, selectCount + selectSize - 1);
                if (listMsg == null)
                    return null;
                
                for (int i = 0; i < listMsg.size(); i++)
                {
                    pTime = getMsgProcessedTime(userId, listMsg.get(i));
                    if (pTime == null)
                        return null;
                    minPTime = Math.min(minPTime, pTime);
                }
                
                selectCount += selectSize;
            }
        }
        
        return minPTime;
    }
    
    public Long getMaxProcessedTimeBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> listUser = getListUserBySenderID(senderId);
        if (listUser == null)
            return null;
        if (listUser.isEmpty())
            return null;
        
        long maxPTime = 0;
        Long listMsgSize;
        String strlistMsgKey;
        for (Long userId : listUser)
        {
            strlistMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (strlistMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(strlistMsgKey);
            if (listMsgSize == null)
                return null;
            
            long selectSize = 0, selectCount = 0;
            List<Long> listMsg;
            Long pTime;
            while (selectCount < listMsgSize)
            {
                selectSize = Math.min(SIZE_PER_GET, listMsgSize - selectCount);
                listMsg = RedisUtil.zRangeLongValue(strlistMsgKey, selectCount, selectCount + selectSize - 1);
                if (listMsg == null)
                    return null;
                
                for (int i = 0; i < listMsg.size(); i++)
                {
                    pTime = getMsgProcessedTime(userId, listMsg.get(i));
                    if (pTime == null)
                        return null;
                    maxPTime = Math.max(maxPTime, pTime);
                }
                
                selectCount += selectSize;
            }
        }
        
        return maxPTime;
    }
    
    
    
    public Double getAverageProcessedTimeBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> listUser = getListUserBySenderID(senderId);
        if (listUser == null)
            return null;
        if (listUser.isEmpty())
            return null;
        
        long sumPTime = 0, totalMsg = 0;
        Long listMsgSize;
        String strlistMsgKey;
        for (Long userId : listUser)
        {
            strlistMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (strlistMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(strlistMsgKey);
            if (listMsgSize == null)
                return null;
            
            long selectSize = 0, selectCount = 0;
            List<Long> listMsg;
            Long pTime;
            while (selectCount < listMsgSize)
            {
                selectSize = Math.min(SIZE_PER_GET, listMsgSize - selectCount);
                listMsg = RedisUtil.zRangeLongValue(strlistMsgKey, selectCount, selectCount + selectSize - 1);
                if (listMsg == null)
                    return null;
                
                for (int i = 0; i < listMsg.size(); i++)
                {
                    pTime = getMsgProcessedTime(userId, listMsg.get(i));
                    if (pTime == null)
                        return null;
                    sumPTime += pTime;
                    totalMsg++;
                }
                
                selectCount += selectSize;
            }
        }
        
        if (totalMsg == 0)
            return null;
        
        return ((double) sumPTime / (double) totalMsg);
    }
        
    public Set<Long> getListSenderByUserID(long userId)
    {
        if (!validateLongValue(userId))
            return null;
              
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
        
        Set<Long> listSender = new HashSet<Long>();
        String msgKey;
        Long senderId;
        for (int msgId = 1; msgId <= msgCounter; msgId++ )
        {
            msgKey = getKeyMsgInfo(userId, msgId);
            senderId = RedisUtil.hGetLongValue(msgKey, RDS_NS_MSG_INFO_FIELD_SENDER_ID);
            if (senderId == null)
                return null;
            
            listSender.add(senderId);
        }
        
        return listSender;            
    }
    
    public Set<Long> getListUserBySenderID(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        String strKey = getKeyListUserBySenderID(senderId);
        if (strKey == null)
            return null;
        
        Set<Long> listUser = RedisUtil.sMembersLongValue(strKey);
        if (listUser == null)
            return null;
        if (listUser.isEmpty())
            return null; // senderid isn't exists
        
        return listUser;
    }
    
}
