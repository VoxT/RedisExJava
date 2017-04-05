/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package http.config.ini;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ini4j.Ini;
import org.ini4j.IniPreferences;
import java.util.prefs.Preferences;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 *
 * @author root
 */
public class INIReaderUtil {
    private static Preferences prefs_ = null;
    public static Preferences getInsIniReader()
    {
        try
        {
            if(prefs_ == null)
                prefs_ = new IniPreferences(new Ini(new File("config.ini")));
            return prefs_;
        }
        catch(Exception ex)
        {
            return null;
        }
    }
    
    public static String getString(String section, String key, String def)
    {
        if(getInsIniReader() == null)
            return def;
        
        return getInsIniReader().node(section).get(key, def);
    }
    
    public static Long getLong(String section, String key, Long def)
    {
        if(getInsIniReader() == null)
            return def;
        
        String sInt = getInsIniReader().node(section).get(key, null);
        if(sInt == null || sInt.isEmpty())
            return def;
        
        Long res = tryParseLong(sInt);
        if(res == null)
            return def;
        
        return res;
    }
    
    //http://www.java2s.com/Code/Java/Regular-Expressions/Extractasubstringbymatchingaregularexpression.htm
    public static List<Integer> getListInteger(String section, String key)
    {
        List<Integer> res = new ArrayList<Integer>();
        
        String iniStr = getString(section, key, "");
        if(iniStr == null || iniStr.isEmpty())
            return res;
        
        Pattern pattent = Pattern.compile("[0-9]+");
        Matcher mat = pattent.matcher(iniStr);
        Integer iter = 0;
        while (mat.find())
        {
            iter = tryParseInteger(mat.group());
            if(iter != null)
                res.add(iter);
        }
        
        return res;
    }
    
    private static Long tryParseLong(String text) {
        try {
            if(text == null)
                return null;
            return Long.parseLong(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
    
    private static Integer tryParseInteger(String text) {
        try {
            if(text == null)
                return null;
            return Integer.parseInt(text);
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
