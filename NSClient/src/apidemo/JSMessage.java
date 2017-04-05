/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package apidemo;

/**
 *
 * @author root
 */
public class JSMessage {
    public Long userId;
    public Long senderId;
    public String data;

    public JSMessage(Long userId, Long senderId, String data) {
        this.userId = userId;
        this.senderId = senderId;
        this.data = data;
    }
}
