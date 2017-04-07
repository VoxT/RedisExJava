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
import java.util.List;
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
            
            if (!saveMsgInfo(jsMsg))
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
    
    private boolean saveMsgInfo(JSMessage jsMsg)
    {
        if (jsMsg == null)
            return false;
        
        System.out.println(RedisUtil.hGetLongValue("myhash", "data"));
//        Long msgId = RedisUtil.incr(RDS_NS_MSG_ID_INCR);
//        if (msgId == null)
//            return false;
//        
//        if (!RedisUtil.hSet(getMsgKey(msgId), RDS_NS_MSG_INFO_FIELD_DATA, jsMsg.data))
//            return false;
//        
//        if (RedisUtil.sAdd(RDS_NS_USERS, Long.toUnsignedString(jsMsg.userId)) == null)
//            return false;
//        
//        if (RedisUtil.zAdd(getKeyListMsgByUserID(jsMsg.userId), 1234, Long.toUnsignedString(msgId)) == null)
//            return false;
//    
//        System.out.println(RedisUtil.hGetStringValue(getMsgKey((long) 2), RDS_NS_MSG_INFO_FIELD_DATA));
//        Set<Long> sm = RedisUtil.sMembersLongValue(RDS_NS_USERS);
//        System.out.println(sm);
//        System.out.println(RedisUtil.sCard(RDS_NS_USERS));
//        List<Long> zm = RedisUtil.zRangeLongValue(getKeyListMsgByUserID((long) 30), 0, -1);
//        System.out.println(RedisUtil.zCard(getKeyListMsgByUserID((long) 30)));
//        System.out.println(RedisUtil.zScore(getKeyListMsgByUserID((long) 30), Long.toString(4)));
//        System.out.println(zm);
        return true;
    }
    
    private String getMsgKey(Long msgId)
    {
        String strMsgId = Long.toUnsignedString(msgId);
        return ("ns:msg:" + strMsgId + ":info");
    }

    private String getKeyListMsgByUserID(Long userId)
    {
        String strUserId = Long.toUnsignedString(userId);
        return ("ns:msg_list_of_user:" + strUserId);
    }

    private String getKeyListMsgBySenderID(Long senderId)
    {
        String strSenderId = Long.toUnsignedString(senderId);
        return ("ns:msg_list_of_sender:" + strSenderId);
    }
    
    
}
