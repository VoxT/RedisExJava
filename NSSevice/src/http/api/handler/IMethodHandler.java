package http.api.handler;

import javax.servlet.http.HttpServletRequest;

public interface IMethodHandler {

    public static final String HEADER_JSON = "application/json;charset=UTF-8";
    public static final String HEADER_HTML = "text/html; charset=utf-8";

    String doAction(HttpServletRequest req);

    String getContentType();
    
    String handleResult(short resultCode, short responseCode, String message);
}
