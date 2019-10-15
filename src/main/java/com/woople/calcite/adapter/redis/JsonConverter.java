package com.woople.calcite.adapter.redis;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Map;

public class JsonConverter {
    private static Gson gsonInstance = new GsonBuilder().disableHtmlEscaping().create();
    private static TypeToken<Map<String, String>> mapType = new TypeToken<Map<String, String>>() {};

    public static String convertMapToJsonStr(Map<String, String> map){
        return gsonInstance.toJson(map);
    }

    public static Map<String, String> convertJsonStrToMap(String jsonStr){
        return gsonInstance.fromJson(jsonStr, mapType.getType());
    }
}
