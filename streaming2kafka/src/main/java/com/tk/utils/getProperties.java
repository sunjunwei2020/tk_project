package com.tk.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

public class getProperties {
    //返回propertites中的所有配置
    public static HashMap<String, String> getProperties(String fileName) throws IOException {
        HashMap<String, String> ppsHashMap = new HashMap<>();
        Properties pps = new Properties();
        pps.load(new FileInputStream(fileName));
        Enumeration<?> enumeration = pps.propertyNames();
        while(enumeration.hasMoreElements()){
            String strKey = (String) enumeration.nextElement();
            String strValue = pps.getProperty(strKey);
            ppsHashMap.put(strKey,strValue);
        }
        return ppsHashMap;
    }
    //返回propertites中的指定key的配置
    public static String getPropertiesKey(String fileName,String strKey) throws IOException {
        Properties pps = new Properties();
        pps.load(new FileInputStream(fileName));
        Enumeration<?> enumeration = pps.propertyNames();
        String strValue = pps.getProperty(strKey);
        return strValue;
    }

}
