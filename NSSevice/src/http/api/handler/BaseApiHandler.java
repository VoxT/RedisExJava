/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.api.handler;

/**
 *
 * @author root
 */
public abstract class BaseApiHandler implements IMethodHandler {
    @Override
    public String getContentType() {
        return HEADER_JSON;
    }
    
    @Override
    public String handleResult(short resultCode, short responseCode, String message)
    {
        return ("{\"result\":" + Short.toString(resultCode)
                + ",\"code\":" + Short.toString(responseCode)
                + ",\"msg\":\"" + message + "\"}");
    }
}