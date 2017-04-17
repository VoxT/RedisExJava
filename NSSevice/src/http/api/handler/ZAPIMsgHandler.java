/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.api.handler;

import http.api.define.ZAPIDefine;
import http.api.define.JSMessage;
import http.api.utils.CommonUtils;
import http.api.utils.GsonUtils;
import http.redis.util.RedisUtil;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
            
            InputStream is = req.getInputStream();
            ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();

            byte buf[] = new byte[ZAPIDefine.MAX_REQ_BUFFER_SIZE];
            int readSize = 0;

            while ((readSize = is.read(buf)) > 0) {
                baOutStream.write(buf, 0, readSize);
            }

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
                                                "Invalid id");
            
            //do someting
            System.out.println("sid=" + jsMsg.senderId + " uid=" + jsMsg.userId 
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
    
    private boolean validateLongValue(Long... value)
    {
        if (value == null)
            return false;
        for (Long val : value)
        {
            if ((val == null) || (val < 0))
                return false;
        }
        
        return true;
    }
    
    private boolean proccessData(JSMessage jsMsg)
    {
        if (jsMsg == null)
            return false;
        
        return ((System.currentTimeMillis() % 2) == 1);
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
        if (RedisUtil.zAdd(listMsgIdKey, 1, Long.toUnsignedString(msgId)) == null)
            return false;
        
        // SET users by senderid
        String listUIdBySIdKey = getKeyListUserBySenderID(jsMsg.senderId);
        if (listUIdBySIdKey == null)
            return false;
        if (RedisUtil.sAdd(listUIdBySIdKey, Long.toUnsignedString(jsMsg.userId)) == null)
            return false;
                
        // ZSET users
        if (RedisUtil.zAdd(RDS_NS_USERS, 1, Long.toUnsignedString(jsMsg.userId)) == null)
            return false;
        // ZSET senders
        if (RedisUtil.zAdd(RDS_NS_SENDERS, 1, Long.toUnsignedString(jsMsg.senderId)) == null)
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
    
    private Long getOperateListMsgFieldByUser(Function func, long userId, Boolean state, long beginValue)
    {
        if (!validateLongValue(userId))
            return null;
        
        Long msgCounter = getUserMsgCounter(userId);
        if (msgCounter == null)
            return null;
                
        for (long msgId = 1; msgId <= msgCounter; msgId++)
        {
            beginValue = func.excuteFunc(userId, msgId, state, beginValue);
        }
        
        return beginValue;
    }
    
    private Long getOperateListMsgFieldBySender(Function func, long senderId, Boolean state, long beginValue)
    {
        try
        { 
            if (!validateLongValue(senderId))
                return null;

            Set<Long> listUser = getListUserBySenderID(senderId);
            if (listUser == null || listUser.isEmpty())
                return null;

            Long listMsgSize;
            String listMsgKey = null;
            for (Long userId : listUser)
            {
                listMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
                if (listMsgKey == null)
                    return null;

                listMsgSize = RedisUtil.zCard(listMsgKey);
                if (listMsgSize == null)
                    return null;

                long selectSize = 0, selectCount = 0;
                List<Long> listMsg = null;
                Boolean result;
                while (selectCount < listMsgSize)
                {
                    selectSize = Math.min(SIZE_PER_GET, listMsgSize - selectCount);
                    listMsg = RedisUtil.zRangeLongValue(listMsgKey, selectCount, selectCount + selectSize - 1);
                    if (listMsg == null)
                        return null;

                    for (int i = 0; i < listMsg.size(); i++)
                    {
                        beginValue = func.excuteFunc(userId, listMsg.get(i), state, beginValue);
                        if (beginValue == -1L)
                            return null;
                    }

                    selectCount += selectSize;
                }
            }

            return beginValue;
        }
        catch (Exception ex)
        {
            return null;
        }
    }
    
    private interface Function 
    {
        long excuteFunc(long userId, long msgId, Boolean state, long beginValue);
    }
    
    private Function getTotalResultByState = new Function() {
        /**
         * 
         * @param userId
         * @param msgId
         * @param state
         * @param beginValue
         * @return -1L if failed
         */
        @Override
        public long excuteFunc(long userId, long msgId, Boolean state, long beginValue) {
            if (state == null)
                return -1L;
            
            Boolean result = getMsgResult(userId, msgId);
            if(result == null)
                return -1L;
            if (result == state)
                beginValue++;
            
            return beginValue;
        }
    };
    
    
    private Function getStatisticalProcessedTime = new Function() {
        /**
        * 
        * @param statePTime
        * @param pTime
        * @param state
        *          null get sum processed time
        *          true get min processed time
        *          false get max processed time
        * @return -1L if failed
        */
        @Override
        public long excuteFunc(long userId, long msgId, Boolean state, long beginValue) {
            Long pTime = getMsgProcessedTime(userId, msgId);
            if (pTime == null)
                return -1L;
            
            if (state == null)
            {
                beginValue += pTime;
            }
            else 
            if (state)
                beginValue = Math.min(beginValue, pTime);
            else beginValue = Math.max(beginValue, pTime);

            return beginValue;
        }
    };
    
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
    
    private Long getMsgProcessedTime(long userId, long msgId)
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

    public Long getUserMsgCounter(long userId) 
    {
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
        try
        {
            Long totalUser = getTotalUser();
            if (totalUser == null)
                return null;

            long selectSize = 0, selectCount = 0, totalReq = 0;
            List<Long> listUser = null;
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
        catch (Exception ex)
        {
            return null;
        }
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
        
        return getOperateListMsgFieldByUser(getTotalResultByState, userId, true, 0);
    }
    
    public Long getTotalFailedByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        return getOperateListMsgFieldByUser(getTotalResultByState, userId, false, 0);
    }
    
    public Long getTotalMsgBySender(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        Set<Long> setUser = getListUserBySenderID(senderId);
        if (setUser == null || setUser.isEmpty())
            return null;
        
        long totalMsg = 0;
        Long listMsgSize;
        String listMsgKey = null;
        for (Long userId : setUser)
        {
            listMsgKey = getKeyListMsgOfSIdAndUId(userId, senderId);
            if (listMsgKey == null)
                return null;
            
            listMsgSize = RedisUtil.zCard(listMsgKey);
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
        
        return getOperateListMsgFieldBySender(getTotalResultByState, senderId, Boolean.TRUE, 0);
    }
    
    public Long getTotalFailedBySender(long senderId)
    {
        if (!validateLongValue(senderId))
                return null;
        
        return getOperateListMsgFieldBySender(getTotalResultByState, senderId, Boolean.FALSE, 0);
    }
    
    public Long getMinProcessedTimeByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        return getOperateListMsgFieldByUser(getStatisticalProcessedTime, userId, Boolean.TRUE, Long.MAX_VALUE);
    }
    
    public Long getMaxProcessedTimeByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        return getOperateListMsgFieldByUser(getStatisticalProcessedTime, userId, Boolean.FALSE, 0);
    }
    
    public Double getAverageProcessedTimeByUser(long userId)
    {
        if (!validateLongValue(userId))
            return null;
        
        return ((double) getOperateListMsgFieldByUser(getStatisticalProcessedTime, userId, null, 0) 
                / (double) getTotalMsgByUser(userId));
    }   
           
    public Long getMinProcessedTimeBySender(long senderId)
    {
        if (!validateLongValue(senderId))
                return null;
        
        return getOperateListMsgFieldBySender(getStatisticalProcessedTime, senderId, Boolean.TRUE, Long.MAX_VALUE);
    }
    
    public Long getMaxProcessedTimeBySender(long senderId)
    {
        if (!validateLongValue(senderId))
                return null;
        
        return getOperateListMsgFieldBySender(getStatisticalProcessedTime, senderId, Boolean.FALSE, 0);
    }
        
    public Double getAverageProcessedTimeBySender(long senderId)
    {
        if (!validateLongValue(senderId))
                return null;
        
        return ( (double) getOperateListMsgFieldBySender(getStatisticalProcessedTime, senderId, null, 0)
                / (double) getTotalMsgBySender(senderId));
    }
        
    public Set<Long> getListSenderByUserID(long userId)
    {
        try {
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
        catch (Exception ex)
        {
            return null;
        }
    }
    
    public Set<Long> getListUserBySenderID(long senderId)
    {
        if (!validateLongValue(senderId))
            return null;
        
        String listUserKey = getKeyListUserBySenderID(senderId);
        if (listUserKey == null)
            return null;
        
        Set<Long> listUser = RedisUtil.sMembersLongValue(listUserKey);
        if (listUser == null || listUser.isEmpty())
            return null; // senderid doesn't exist.
        
        return listUser;
    }
    
}
