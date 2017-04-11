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
    private static final String RDS_NS_MSG_ID_INCR                         = "ns:msg_id_incr";
    private static final String RDS_NS_MSG_INFO_FIELD_DATA                 = "data";
    private static final String RDS_NS_MSG_INFO_FIELD_SENDER_ID            = "senderid";
    private static final String RDS_NS_MSG_INFO_FIELD_USER_ID              = "userid";
    private static final String RDS_NS_MSG_INFO_FILED_PROCESSED_RESULT     = "result";
    private static final String RDS_NS_MSG_INFO_FIELD_REQUEST_TIME         = "reqtime";
    private static final String RDS_NS_MSG_INFO_FIELD_SEND_TIME            = "sendtime";

    private static final String RDS_NS_USERS                               = "ns:users";
    private static final String RDS_NS_SENDERS                             = "ns:senders";

    private static final String RDS_NS_REQ_COUNTER_FAILED                  = "ns:request_counter_failed";
    private static final String RDS_NS_REQ_COUNTER_TOTAL                   = "ns:request_counter_total";

    private static final String RDS_NS_PROCESSED_TIME_MAX                  = "ns:processed_time_max";
    private static final String RDS_NS_PROCESSED_TIME_MIN                  = "ns:processed_time_min";
    private static final String RDS_NS_PROCESSED_TIME_AVERAGE              = "ns:processed_time_average";

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
            
            final int contentLength = req.getContentLength();
            if (contentLength > ZAPIDefine.MAX_REQ_BUFFER_SIZE)
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_REQ_SIZE_TOO_BIG, 
                                                "Request size too big");
            
            // Read request data
            InputStream is = req.getInputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            byte buf[] = new byte[ZAPIDefine.MAX_REQ_BUFFER_SIZE];
            int readBytes = 0, countBytes = 0;

            while (countBytes < contentLength) {
                readBytes = is.read(buf);
                baos.write(buf, 0, readBytes);
                countBytes += readBytes;
            }
            if (countBytes != contentLength)
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_RES_READ_REQ_FAILED, 
                                                "Read request failed");

            // parse json
            String js = new String(baos.toByteArray(), Charset.forName("UTF-8"));
            JSMessage jsMsg = GsonUtils.fromJsonString(js, JSMessage.class);
            if ((jsMsg.senderId <= 0) || (jsMsg.userId <= 0))
                return CommonUtils.handleResult(ZAPIDefine.API_RESULT_FAIL, 
                                                ZAPIDefine.API_INVALID_ID, 
                                                "Invalid id uId=" + Long.toString(jsMsg.userId) 
                                                + " sId=" + Long.toString(jsMsg.senderId));
            
            //do someting
            System.out.println(jsMsg.senderId + " " + jsMsg.data 
                    + " " + jsMsg.userId);
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
                                            "opp! something went wrong" + ex.getMessage());
        }
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
        
        Long msgId = saveMsgInfo(jsMsg, reqTime, sendTime, result);
        if (msgId == null)
            return false;
        
        if (RedisUtil.sAdd(RDS_NS_USERS, Long.toUnsignedString(jsMsg.userId)) == null)
            return false;
        if (RedisUtil.sAdd(RDS_NS_SENDERS, Long.toUnsignedString(jsMsg.senderId)) == null)
            return false;
        
        if (RedisUtil.zAdd(getKeyListMsgByUserID(jsMsg.userId), reqTime, Long.toUnsignedString(msgId)) == null)
            return false;
        if (RedisUtil.zAdd(getKeyListMsgBySenderID(jsMsg.senderId), sendTime, Long.toUnsignedString(msgId)) == null)
            return false;
        
        // request counter
        if (!UpdateRequestCounter(result))
            return false;

        // min, max, average processed time
        if (!updateProcessedTime(sendTime - reqTime))
            return false;

        return true;
    }
    
    private String getMsgKey(long msgId)
    {
        String strMsgId = Long.toUnsignedString(msgId);
        return ("ns:msg:" + strMsgId + ":info");
    }

    private String getKeyListMsgByUserID(long userId)
    {
        String strUserId = Long.toUnsignedString(userId);
        return ("ns:msg_list_of_user:" + strUserId);
    }

    private String getKeyListMsgBySenderID(long senderId)
    {
        String strSenderId = Long.toUnsignedString(senderId);
        return ("ns:msg_list_of_sender:" + strSenderId);
    }

    private Long saveMsgInfo(JSMessage jsMsg, long reqTime, long sendTime, boolean result) {
        Long msgId = RedisUtil.incr(RDS_NS_MSG_ID_INCR);
        if (msgId == null)
            return null;
        
        String strHash = getMsgKey(msgId);
        if (!RedisUtil.hSet(strHash, RDS_NS_MSG_INFO_FIELD_USER_ID, Long.toUnsignedString(jsMsg.userId)))
            return null;
        if (!RedisUtil.hSet(strHash, RDS_NS_MSG_INFO_FIELD_SENDER_ID, Long.toUnsignedString(jsMsg.senderId)))
            return null;
        if (!RedisUtil.hSet(strHash, RDS_NS_MSG_INFO_FIELD_DATA, jsMsg.data))
            return null;
        if (!RedisUtil.hSet(strHash, RDS_NS_MSG_INFO_FILED_PROCESSED_RESULT, result ? "1":"0"))
            return null;
        if (!RedisUtil.hSet(strHash, RDS_NS_MSG_INFO_FIELD_REQUEST_TIME, Long.toUnsignedString(reqTime)))
            return null;
        if (!RedisUtil.hSet(strHash, RDS_NS_MSG_INFO_FIELD_SEND_TIME, Long.toUnsignedString(sendTime)))
            return null;
        
        return msgId;
    }

    private boolean UpdateRequestCounter(boolean result) {
        if (RedisUtil.incr(RDS_NS_REQ_COUNTER_TOTAL) == null)
            return false;
    
        if (!result)
        {
            if (RedisUtil.incr(RDS_NS_REQ_COUNTER_FAILED) == null)
                return false;
        }

        return true;
    }

    private boolean updateProcessedTime(long pTime) {
        if (pTime < 0)
            return false;
        // processed time
        Long isExists = RedisUtil.isExistKey(RDS_NS_PROCESSED_TIME_AVERAGE);
        if (isExists == null)
            return false;

        // Set key if key isn't exists
        if (isExists == 0)
            return setProcessedTime(pTime);

        String strPTime = Long.toUnsignedString(pTime);
        // Set max processed time
        Long maxTime = getMaxProcessedTime();
        if (maxTime == null)
            return false;
        if (pTime > maxTime)
        {
            if (!RedisUtil.setStringValue(RDS_NS_PROCESSED_TIME_MAX, strPTime))
                return false;
        }

        // Set min processed time
        Long minTime = getMinProcessedTime();
        if (minTime == null)
            return false;
        if (pTime < minTime)
        {
            if (!RedisUtil.setStringValue(RDS_NS_PROCESSED_TIME_MIN, strPTime))
                return false;
        }

        // Set average processed time
        Long reqTotal = getTotalRequest();
        if (reqTotal == null)
            return false;
        if (reqTotal == 0)
            return false;
        if (reqTotal == 1)
            return true;

        // cal average processed time
        Long avgTime = getAverageProcessedTime();
        if (avgTime == null)
            return false;

        avgTime = (long) ((((avgTime*(reqTotal - 1)) + pTime) / reqTotal) + 0.5);
        if (!RedisUtil.setStringValue(RDS_NS_PROCESSED_TIME_AVERAGE, Long.toUnsignedString(avgTime)))
            return false;

        return true;
    }

    private boolean setProcessedTime(long pTime) {
        String strPTime = Long.toUnsignedString(pTime);
        if (!RedisUtil.setStringValue(RDS_NS_PROCESSED_TIME_MAX, strPTime))
            return false;
        if (!RedisUtil.setStringValue(RDS_NS_PROCESSED_TIME_MIN, strPTime))
            return false;
        if (!RedisUtil.setStringValue(RDS_NS_PROCESSED_TIME_AVERAGE, strPTime))
            return false;

        return true;
    }

    public Long getMaxProcessedTime() {
        return RedisUtil.getLongvalue(RDS_NS_PROCESSED_TIME_MAX);
    }

    public Long getMinProcessedTime() {
        return RedisUtil.getLongvalue(RDS_NS_PROCESSED_TIME_MIN);
    }

    public Long getTotalRequest() {
        return RedisUtil.getLongvalue(RDS_NS_REQ_COUNTER_TOTAL);
    }

    public Long getAverageProcessedTime() {
        return RedisUtil.getLongvalue(RDS_NS_PROCESSED_TIME_AVERAGE);
    }
    
    public Set<Long> getListUser()
    {
        return RedisUtil.sMembersLongValue(RDS_NS_USERS);
    }
    
    public Set<Long> getListSender()
    {
        return RedisUtil.sMembersLongValue(RDS_NS_SENDERS);
    }
    
    public List<Long> getListMsgByUser(long userId)
    {
        if (userId <= 0)
            return null;
        
        return RedisUtil.zRangeLongValue(getKeyListMsgByUserID(userId), 0, -1);
    }
    
    public List<Long> getListMsgBySender(long senderId)
    {
        if (senderId <= 0)
            return null;
        
        return RedisUtil.zRangeLongValue(getKeyListMsgBySenderID(senderId), 0, -1);
    }
    
    public Set<Long> getListSenderByUser(long userId)
    {
        if (userId <= 0)
            return null;
        
        List<Long> msgList = getListMsgByUser(userId);
        if (msgList == null)
            return null;
        
        Set<Long> users = new HashSet<>();
        Long uId;
        for (Long msgId : msgList) 
        {
            if (msgId == null)
                continue;
            
            uId = RedisUtil.hGetLongValue(getMsgKey(msgId), RDS_NS_MSG_INFO_FIELD_SENDER_ID);
            if (uId == null)
                continue;
            
            users.add(uId);
        }
        
        return users;
    }
    
    public Set<Long> getListUserBySender(long senderId)
    {
        if (senderId <= 0)
            return null;
        
        List<Long> msgList = getListMsgBySender(senderId);
        if (msgList == null)
            return null;
        
        Set<Long> senders = new HashSet<>();
        Long sId;
        for (Long msgId : msgList) 
        {
            if (msgId == null)
                continue;
            
            sId = RedisUtil.hGetLongValue(getMsgKey(msgId), RDS_NS_MSG_INFO_FIELD_USER_ID);
            if (sId == null)
                continue;
            
            senders.add(sId);
        }
        
        return senders;
    }
}
