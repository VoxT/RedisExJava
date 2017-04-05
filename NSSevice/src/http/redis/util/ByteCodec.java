/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.redis.util;

import com.lambdaworks.redis.codec.RedisCodec;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author root
 */
//public class ByteCodec extends RedisCodec<String, byte[]> {
public class ByteCodec implements RedisCodec<String, byte[]> {
    private Charset charset = Charset.forName("UTF-8");

    @Override
    public String decodeKey(ByteBuffer bb) {
        String converted = "";
        try {
            converted = new String(bb.array(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(ByteCodec.class.getName()).log(Level.SEVERE, null, ex);
        }
        return converted;
    }

    @Override
    public byte[] decodeValue(ByteBuffer bb) {
        return decode(bb);
    }

    @Override
    public ByteBuffer encodeKey(String k) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(k.getBytes("UTF-8"));
            return buffer;
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(ByteCodec.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    @Override
    public ByteBuffer encodeValue(byte[] v) {
        return ByteBuffer.wrap(v);
    }
    
    private byte[] decode(ByteBuffer bytes) {
   	 try {
            byte[] ba = new byte[bytes.remaining()];
            bytes.get(ba);
            return ba;
        } catch (Exception e) {
            return null;
        }
    }
}
