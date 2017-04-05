/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.config;

/**
 *
 * @author root
 */
public class ZServerInfo {
    private String host = "";
    private int port = 0;
    
    public ZServerInfo(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        if(port > 0)
            this.port = port;
        else
            this.port = 0;
    }
}
