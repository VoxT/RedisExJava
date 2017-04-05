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
public class ZAPIDefine {
    public static final short API_RESULT_SUCCESS                  = 1;
    public static final short API_RESULT_FAIL                     = 0;
    
    public static final short API_RES_SUCCESS                     = 0;
    public static final short API_RES_WRONG_VERSION               = 400;
    public static final short API_RES_WRONG_METHOD                = 401;
    public static final short API_RES_SAVE_MSG_FAIL               = 402;
    public static final short API_RES_WRONG_API                   = 404;
    public static final short API_INVALID_SIGNATURE               = 403;
    public static final short API_RES_PARSE_FAIL                  = 405;
    public static final short API_RES_MISS_DATA                   = 406;
    public static final short API_RES_REQ_SIZE_TOO_BIG            = 407;
    public static final short API_RES_READ_REQ_FAILED             = 408;
    public static final short API_EXCEED_MAX_UIDS_SIZE            = 409;

    public static final short API_RES_MTU_SENT                    = 51;
    public static final short API_RES_MTU_SAVED                   = 52;
    public static final String API_RES_NS_CODE                    = "code";

    public static final String API_PATH_NSSERVICE                 = "nsservice";
    public static final String API_PATH                           = "api";
    public static final String API_PATH_MSG                       = "msg";

    public static final short MAX_REQ_BUFFER_SIZE                 = 10240;
}
