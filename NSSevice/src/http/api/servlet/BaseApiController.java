package http.api.servlet;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import http.api.handler.IMethodHandler;
import http.api.handler.MethodHandlerFactory;
import http.api.utils.CommonUtils;

public class BaseApiController {

    private static final Logger logger = Logger.getLogger(BaseApiController.class);
    private static final Logger requestUrlLog = Logger.getLogger("requestUrlLog");
    private static final Lock createLock = new ReentrantLock();
    private static BaseApiController instance = null;

    public static BaseApiController getInstance() {
        if (instance == null) {
            createLock.lock();
            try {
                if (instance == null) {
                    instance = new BaseApiController();
                }
            } finally {
                createLock.unlock();
            }
        }
        return instance;
    }

    public void handle(HttpServletRequest req, HttpServletResponse resp) {        
        resp.setContentType("application/json;charset=UTF-8");
        try {
            String result = this.doHandle(req, resp);            
            this.outAndClose(result, req, resp);
        } catch (Exception ex) {
            logger.error("doHandle exception " + ex.getMessage(), ex);
            logger.error("requestURI = " + CommonUtils.getRequestUrl(req));
        }

    }

    private String doHandle(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        List<String> pathAPI = this.parseUriRequest(req);
        
        logger.info("Accept connection from addr=" + req.getRemoteHost() + " uri=" + req.getRequestURI());
        if (pathAPI.size() < 2) {
            return ("Wrong api: uri=" + req.getRequestURI());
        }
        
        //String methodName = params.get(1);
        //IMethodHandler handler = MethodHandlerFactory.getMethodHandler(methodName);
        
        IMethodHandler handler = MethodHandlerFactory.getMethodHandler(pathAPI);
        if (handler == null) {
            return "{result:0,code:404,msg:\"Not match api\"}";
        }
        resp.setContentType(handler.getContentType());
        
        return handler.doAction(req);
    }

    protected List<String> parseUriRequest(HttpServletRequest req) {
        List<String> result = new ArrayList();
        try {
            String uripath = req.getRequestURI();
            String[] splitArray = uripath.split("/");
            for (int i = 1; i < splitArray.length; i++) {
                result.add(splitArray[i]);
            }
        } catch (Exception ex) {
            logger.error("parseUriRequest exception " + ex.getMessage(), ex);
        }
        return result;
    }

    protected void outAndClose(String content, HttpServletRequest req, HttpServletResponse resp) {

        PrintWriter out = null;
        try {
            resp.setCharacterEncoding("UTF-8");
            out = resp.getWriter();
            out.print(content);
        } catch (Exception ex) {
            logger.error("outAndClose exception " + ex.getMessage(), ex);
            logger.error("requestURI=" + req.getRequestURI() + "?" + req.getQueryString());
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
