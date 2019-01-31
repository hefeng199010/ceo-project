package com.tuandai.tools;

import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ConcurrentHashMap;

public class BusinessType {
   private   static ConcurrentHashMap<String,String> map=new ConcurrentHashMap<>();

     static {
        map.put("房速贷标准件", "house");
        map.put("房速贷非标准件", "house");
        map.put("T110", "house");
        map.put("T100", "house");
        map.put("T600", "house");
        map.put("T251", "small");
        map.put("T160", "small");
        map.put("T170", "small");

        map.put("一点车贷", "car");
        map.put("车易贷", "car");
        map.put("T500", "car");
    }

    public static String getType(String business_type){
        String resultType="";
        try {
            if(business_type!="" && business_type!=""  && !StringUtils.isEmpty(business_type)){
                resultType = map.get(business_type);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultType;
    }

    public static void main(String[] args) {
        System.err.println(BusinessType.getType("一点车贷"));
    }
}
