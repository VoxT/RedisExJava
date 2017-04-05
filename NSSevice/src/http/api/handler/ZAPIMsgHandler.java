/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.api.handler;

import http.api.define.ZAPIDefine;
import http.api.define.JSMessage;
import com.google.protobuf.ByteString;
import http.api.utils.GsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import javax.servlet.http.HttpServletRequest;

/**
 *
 * @author root
 */
public class ZAPIMsgHandler extends BaseApiHandler {

    private static final ZAPIMsgHandler instance = new ZAPIMsgHandler();

    private ZAPIMsgHandler() {
    }

    public static ZAPIMsgHandler getInstance() {
        return instance;
    }
    
    @Override
    public String doAction(HttpServletRequest req) {
        try {
            if (req.getMethod().compareToIgnoreCase("POST") != 0)
                return handleResult(ZAPIDefine.API_RESULT_FAIL, 
                        ZAPIDefine.API_RES_WRONG_METHOD, "Wrong method. Must be POST");
            
            InputStream is = req.getInputStream();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            byte buf[] = new byte[2048];
            int letti = 0;

            while ((letti = is.read(buf)) > 0) {
                baos.write(buf, 0, letti);
            }

            String js = new String(baos.toByteArray(), Charset.forName("UTF-8"));
            JSMessage jsMsg = GsonUtils.fromJsonString(js, JSMessage.class);
            
            //do someting
            System.out.println(jsMsg.senderId + " " + jsMsg.data 
                    + " " + jsMsg.userId);
           
            //return json
            return handleResult(ZAPIDefine.API_RESULT_SUCCESS, 
                        ZAPIDefine.API_RES_SUCCESS, "success");

        } catch (Exception ex) {
            return handleResult(ZAPIDefine.API_RESULT_FAIL, 
                        ZAPIDefine.API_RES_READ_REQ_FAILED, "Read request failed");
        }
    }
    
}
