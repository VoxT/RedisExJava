package http.api.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.MalformedChallengeException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

public class CommonUtils {

    private static final Logger logger = Logger.getLogger(CommonUtils.class);

    public static String getParameter(HttpServletRequest req, String paramName) {
        String value = req.getParameter(paramName);
        if (value != null) {
            value = value.trim();
        } else {
            value = "";
        }
        return value;
    }

    public static String getRequestUrl(HttpServletRequest request) {
        StringBuilder stringBuilder = new StringBuilder(request.getRequestURI());
        Enumeration enu = request.getParameterNames();
        stringBuilder.append("?");
        while (enu.hasMoreElements()) {
            String paramName = (String) enu.nextElement();
            stringBuilder.append(paramName).append("=")
                    .append(request.getParameter(paramName))
                    .append("&");
        }
        return stringBuilder.toString();
    }

    public static String removeTabAndNewLine(String str) {
        if (str != null) {
            return str.trim().replaceAll("\r", "").replaceAll("\n", " ").replaceAll("\t", " ");
        } else {
            return "";
        }
    }

    public static String getClientIP(HttpServletRequest request) {
        String ipAddress = "";
        try {
            ipAddress = request.getHeader("X-FORWARDED-FOR");
            if (ipAddress == null) {
                ipAddress = request.getRemoteAddr();
            }
            //mot so truong hop ipAddress co dang "118.71.76.149, 198.23.90.140";
            int i = ipAddress.indexOf(",");
            if (i > -1) {
                if (i > 0) {
                    ipAddress = ipAddress.substring(0, i).trim();
                } else {
                    ipAddress = ipAddress.substring(i + 1, ipAddress.length()).trim();
                }
            }
        } catch (Exception ex) {
            logger.error("getClientIP exception " + ex.getMessage(), ex);
        }
        return ipAddress;
    }

    private static String serverIP = null;

    public static String getServerIP() {
        if (serverIP == null) {
            try {
                Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
                while (n.hasMoreElements()) {
                    NetworkInterface e = n.nextElement();
                    Enumeration<InetAddress> a = e.getInetAddresses();
                    while (a.hasMoreElements()) {
                        InetAddress addr = a.nextElement();
                        if (addr.isLinkLocalAddress() == false && addr.isLoopbackAddress() == false && addr.isSiteLocalAddress() == true) {
                            serverIP = addr.getHostAddress();
                            return serverIP;
                        }
                    }
                }
            } catch (Exception ex) {
                logger.error("getServerIP exception " + ex.getMessage(), ex);
                try {
                    serverIP = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException ex1) {
                    logger.error("getServerIP exception " + ex1.getMessage(), ex1);
                    //quá đen !!!
                    serverIP = "127.0.0.1";
                }
            }
        }
        return serverIP;
    }

    public static String getYYYYMMfromTransID(long transID) {
        String transIDString = String.valueOf(transID);
        return getYYYYMMfromTransID(transIDString);
    }

    public static String getYYYYFromTime(long time) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY");
        return simpleDateFormat.format(new Date(time));
    }

    public static String getYYYYMMfromTransID(String transID) {
        String transIDString = String.valueOf(transID);
        if (transIDString.length() != 15) {
            throw new IllegalArgumentException("invalid transID " + transID);
        }
        return "20" + transIDString.substring(0, 4);
    }

    public static String getYYMMDDfromAppTransID(String appTransID) {

        if (appTransID == null || appTransID.length() < 6) {
            throw new IllegalArgumentException("invalid appTransID " + appTransID);
        }
        return "20" + appTransID.substring(0, 4);
    }

    public static long getDefaultTransID() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyMMdd");
        long transID = Long.parseLong(sdf.format(new Date())) * 1000000000;
        return transID;
    }

    public static boolean isEmpty(String pStr) {
        if (pStr == null || pStr.length() == 0) {
            return true;
        }
        return false;
    }

    public static String sendPost(List<NameValuePair> nvps, String postUrl, int timeout) throws UnsupportedEncodingException, IOException {
        String result;
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setStaleConnectionCheckEnabled(true)
                .build();
        try (CloseableHttpClient httpclient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpPost httpPost = new HttpPost(postUrl);
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));
            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed : HTTP getStatusCode: "
                            + response.getStatusLine().getStatusCode() + " HTTP getReasonPhrase: " + response.getStatusLine().getReasonPhrase());
                }
                HttpEntity entity = response.getEntity();
                try (InputStream inputStream = entity.getContent()) {
                    result = IOUtils.toString(inputStream, "UTF-8");
                }
            }
        }
        return result;
    }

    public static String sendPostJson(String postUrl, String jsonContent, int timeout)
            throws Exception {
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpPost httpPost = new HttpPost(postUrl);
            StringEntity input = new StringEntity(jsonContent, "UTF-8");
            input.setContentType("application/json");
            httpPost.setEntity(input);
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed : HTTP getStatusCode: "
                            + response.getStatusLine().getStatusCode()
                            + " HTTP getReasonPhrase: "
                            + response.getStatusLine().getReasonPhrase());
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())))) {
                    String output;
                    StringBuilder strBuilder = new StringBuilder();
                    while ((output = br.readLine()) != null) {
                        strBuilder.append(output);
                    }
                    return strBuilder.toString();
                }
            }
        }
    }

    public static String sendPostJson(String postUrl, String jsonContent, int timeout, String contentType)
            throws Exception {
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpPost httpPost = new HttpPost(postUrl);
            StringEntity input = new StringEntity(jsonContent, "UTF-8");
            input.setContentType(contentType);
            httpPost.setEntity(input);
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed : HTTP getStatusCode: "
                            + response.getStatusLine().getStatusCode()
                            + " HTTP getReasonPhrase: "
                            + response.getStatusLine().getReasonPhrase());
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())))) {
                    String output;
                    StringBuilder strBuilder = new StringBuilder();
                    while ((output = br.readLine()) != null) {
                        strBuilder.append(output);
                    }
                    return strBuilder.toString();
                }
            }
        }
    }

    public static String sendPostJsonViaProxy(String postUrl, String jsonContent, String proxyHost, int proxyPort, int timeout)
            throws Exception {
        HttpHost proxy = new HttpHost(proxyHost, proxyPort);
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setStaleConnectionCheckEnabled(true)
                .setProxy(proxy)
                .build();
        try (CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpPost httpPost = new HttpPost(postUrl);
            StringEntity input = new StringEntity(jsonContent, "UTF-8");
            input.setContentType("application/json");
            httpPost.setEntity(input);
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed : HTTP getStatusCode: "
                            + response.getStatusLine().getStatusCode()
                            + " HTTP getReasonPhrase: "
                            + response.getStatusLine().getReasonPhrase());
                }
                try (BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())))) {
                    String output;
                    StringBuilder strBuilder = new StringBuilder();
                    while ((output = br.readLine()) != null) {
                        strBuilder.append(output);
                    }
                    return strBuilder.toString();
                }
            }
        }
    }

    public static String sendGet(String url, int timeout) throws IOException {
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setStaleConnectionCheckEnabled(true)
                .build();

        try (CloseableHttpClient httpclient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpGet request = new HttpGet(url);
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed : HTTP getStatusCode: "
                            + response.getStatusLine().getStatusCode()
                            + " HTTP getReasonPhrase: "
                            + response.getStatusLine().getReasonPhrase());
                }
                try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    StringBuilder result = new StringBuilder();
                    String line;
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                    return result.toString();
                }
            }
        }
    }

    public static String getParameterAsString(HttpServletRequest request) {
        StringBuilder stringBuilder = new StringBuilder(request.getRequestURI());
        Enumeration enu = request.getParameterNames();
        stringBuilder.append("?");
        while (enu.hasMoreElements()) {
            String paramName = (String) enu.nextElement();
            stringBuilder.append(paramName).append("=")
                    .append(request.getParameter(paramName))
                    .append("&");
        }
        return stringBuilder.toString();
    }

    public static String sendPostViaProxy(List<NameValuePair> nvps, String postUrl, String proxyHost, int proxyPort, int timeout) throws Exception {

        HttpHost proxy = new HttpHost(proxyHost, proxyPort);
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setStaleConnectionCheckEnabled(true)
                .setProxy(proxy)
                .build();
        try (CloseableHttpClient httpclient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpPost httpPost = new HttpPost(postUrl);
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));
            try (CloseableHttpResponse response = httpclient.execute(httpPost)) {
                HttpEntity entity = response.getEntity();
                InputStream inputStream = entity.getContent();;
                String resp = IOUtils.toString(inputStream, "UTF-8");
                return resp;
            }
        }

    }

    public static String sendGetViaProxy(String url, String proxyHost, int proxyPort, int timeout) throws IOException, AuthenticationException, MalformedChallengeException {

        HttpHost proxy = new HttpHost(proxyHost, proxyPort);
        RequestConfig defaultRequestConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setStaleConnectionCheckEnabled(true)
                .setProxy(proxy)
                .build();

        try (CloseableHttpClient httpclient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build()) {
            HttpGet request = new HttpGet(url);
            try (CloseableHttpResponse response = httpclient.execute(request)) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IOException("Failed : HTTP getStatusCode: "
                            + response.getStatusLine().getStatusCode()
                            + " HTTP getReasonPhrase: "
                            + response.getStatusLine().getReasonPhrase());
                }
                try (BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
                    StringBuilder result = new StringBuilder();
                    String line;
                    while ((line = rd.readLine()) != null) {
                        result.append(line);
                    }
                    return result.toString();
                }
            }

        }
    }

    public static String truncateToLength(String longString, int maxLength) {
        if (longString == null) {
            return "";
        }
        if (longString.length() <= maxLength) {
            return longString;
        }
        return longString.substring(0, maxLength);
    }

    public static String getPostData(HttpServletRequest httpServletRequest) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader reader = httpServletRequest.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    public static String decodeUTF8(String input) {
        try {
            return java.net.URLDecoder.decode(input, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.error("decodeUTF8 exception " + ex.getMessage(), ex);
            logger.error("decodeUTF8 input " + input);
            return input;
        }
    }

}
