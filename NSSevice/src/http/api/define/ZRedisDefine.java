/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.api.define;

/**
 *
 * @author Thieu Vo
 */
public class ZRedisDefine {
public static final String RDS_NS_MSG_ID_INCR                         = "ns:msg_id_incr";
public static final String RDS_NS_MSG_INFO_FIELD_DATA                 = "data";
public static final String RDS_NS_MSG_INFO_FIELD_SENDER_ID            = "senderid";
public static final String RDS_NS_MSG_INFO_FIELD_USER_ID              = "userid";
public static final String RDS_NS_MSG_INFO_FILED_PROCESSED_RESULT     = "result";
public static final String RDS_NS_MSG_INFO_FIELD_REQUEST_TIME         = "reqtime";
public static final String RDS_NS_MSG_INFO_FIELD_SEND_TIME            = "sendtime";

public static final String RDS_NS_USERS                               = "ns:users";
public static final String RDS_NS_SENDERS                             = "ns:senders";

public static final String RDS_NS_REQ_COUNTER_FAILED                  = "ns:request_counter_failed";
public static final String RDS_NS_REQ_COUNTER_TOTAL                   = "ns:request_counter_total";

public static final String RDS_NS_PROCESSED_TIME_MAX                  = "ns:processed_time_max";
public static final String RDS_NS_PROCESSED_TIME_MIN                  = "ns:processed_time_min";
public static final String RDS_NS_PROCESSED_TIME_AVERAGE              = "ns:processed_time_average";

public static final int MAX_TIME_EXPIRE_KEY                           = 172800; // seconds (48h)

}
