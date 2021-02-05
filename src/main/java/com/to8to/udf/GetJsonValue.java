package com.to8to.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public final class GetJsonValue {
    /**
     * 正则模式匹配
     * arrayRegex
     * 配置数组，最外面是 [], 最外面是[]的共有3种，归为数组的有2种  数组: 2
     * 1：[[], [], [], ...] 数组里面是数组
     * 2：[{k:v}, {k:v}, ...] 数组里面是字典
     * objectRegex
     * 配置 map 也可能有2种形式 map 3
     * 1: {k1=v1, k2=v2, ...}
     * 2: [{k1=v1, k2=v2, ...}, {k1=v1, k2=v2, ...}, ...]
     * 配置 json 只有一种形式，形如字典 json 1
     * 1: {k1:v1, k2:v2, ...}
     * map 和 json 的区别：k、v之间的连接符号
     */
    private final String arrayRegex = "\\s*\\[.*]\\s*";  // [] 里有若干任意的字符
    private final String objectRegex = "\\s*\\{.*}\\s*"; // {} 里有若干任意的字符
    public int jsonType = -1;

    /**
     * @param _jsonStr 传入的字符串参数
     * @param _key     传入的key参数
     * @return 在 _jsonStr 字符串中找出键 _key 的 value
     */
    @ScalarFunction("getjsonvalue")
    @SqlType(StandardTypes.VARCHAR)
    @Description("getjsonvalue")
    public Slice getJsonValue(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice _jsonStr, Slice _key) {
        String jsonStr = _jsonStr.toStringUtf8(); // Slice转String
        String key = _key.toStringUtf8();
        try {
            /*
             * 把字符串的的回车换行都去掉，并判断该字符串的模式匹配类型
             * json 1
             * 数组: 2
             * map 3
             * 无法识别：0
             */
            jsonStr = jsonStr.replace("\n", "").replace("\r", "");
            jsonType = getJosnType(jsonStr);
            //数组无法获取指定key,无法识别的字符串类型，数组不是键值对结构，无法获取key的value,返回空
            if (jsonType == 2 || jsonType == 0) {
                return Slices.utf8Slice("");
            }

            // [{k1=v1, k2=v2, ...}, {k1=v1, k2=v2, ...}, ...]     {k1=v1, k2=v2, ...}
            if (jsonType == 3) { // 如果是map类型的
                //map 数组类型,无法返回
                if (jsonStr.matches(arrayRegex)) {  // 但是这个map它是用[]包括起来的，相当于数组，没有key，也返回空串
                    return Slices.utf8Slice("");
                }
                // 返回获取map的键值
                return Slices.utf8Slice(getMapValue(jsonStr, key.toLowerCase().trim()));
            }
            //key转小写
            JSONObject object = toLowerKeyObject(JSONObject.parseObject(jsonStr));
            return Slices.utf8Slice(getJsonObjectValue(object, key.toLowerCase().trim()));
        } catch (Exception e) {
            return Slices.utf8Slice("");
        }
    }


    // 处理最外面是[]的，
    // [{：},{:},...] 第几个（arrayIndex）个json{:}
    // [[],[],...] 第几个（arrayIndex）个[]
    // 或者 [{=},{=},{=},...]第几个（arrayIndex）个{=}}
    @ScalarFunction("getjsonvalue")
    @SqlType(StandardTypes.VARCHAR)
    @Description("getjsonvalue")
    public Slice getJsonValue(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice _jsonStr, int arrayIndex) {
        try {
            String jsonStr = _jsonStr.toStringUtf8().replace("\n", "").replace("\r", "");
            jsonType = getJosnType(jsonStr);
            if (jsonType != 2) {// 不是数组
                //map单独处理
                if (jsonType == 3) { // 是map {}和[{},{},...]
                    //对象类型字符串,数组无法返回
                    if (jsonStr.matches(objectRegex)) { // 如果是纯map,也就是外面没有[]的，这个直接用第一个函数去处理，这个函数不处理这一个
                        return null;
                    }
                    return Slices.utf8Slice(getMatchPair(jsonStr, '{', '}', arrayIndex));  // 返回[{：},{:},...]第几个（arrayIndex）个json
                }
                return Slices.utf8Slice("");
            }
            // =2 [[],[],...] 或者 [{},{},{},...]
            try {
                return Slices.utf8Slice(JSONArray.parseArray(jsonStr).get(arrayIndex).toString());

            } catch (IndexOutOfBoundsException e) {
                return Slices.utf8Slice("");
            } catch (Exception e) {
                if (jsonStr.trim().indexOf('[', 1) < 0) {

                    return Slices.utf8Slice(jsonStr.replace("[", "").replace("]", "").split(",")[arrayIndex].trim());  // [{},{},{},...] 返回第几个{}
                }
                return Slices.utf8Slice(getLayerMatchPair(jsonStr, '[', ']', 1).get(arrayIndex));// [[],[],...] 返回第几个[]
            }

        } catch (Exception ee) {
            return Slices.utf8Slice("");
        }
    }

    @ScalarFunction("getjsonvalue")
    @SqlType(StandardTypes.VARCHAR)
    @Description("getjsonvalue")
    public Slice getJsonValue(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice _jsonStr, int arrayIndex, Slice _key) {
        try {
            Slice jsonValueIndex = getJsonValue(_jsonStr, arrayIndex);
            Slice jsonValue = getJsonValue(jsonValueIndex, _key);
            return Slices.utf8Slice(jsonValue.toStringUtf8());
        } catch (Exception e) {
            return Slices.utf8Slice("");
        }
    }


    //获取json对象指定key的value值
    public String getJsonObjectValue(JSONObject jsonObject, String key) {
        //if (key.indexOf(".") == -1) {
        if (!key.contains(".")) {
            return jsonObject.get(key).toString();
        }
        Object object = jsonObject.get(key.substring(0, key.indexOf(".")));
        if (object == null || object.getClass().toString().endsWith("JSONArray")) {

            return null;
        }
        //有时会将字符串对象当成字符串处理
        if (object.getClass().toString().endsWith("String")) {
            object = JSONObject.parseObject(object.toString());

        }
        return getJsonObjectValue((JSONObject) object, key.substring(key.indexOf(".") + 1));

    }

    //json对象key小写
    public JSONObject toLowerKeyObject(JSONObject o1) {
        JSONObject o2 = new JSONObject();
        for (String key : o1.keySet()) {
            Object object = o1.get(key);

            //value值未null
            if (object == null) {
                o2.put(key.toLowerCase(), null);
                continue;
            }

            if (object.getClass().toString().endsWith("JSONObject")) {
                o2.put(key.toLowerCase(), toLowerKeyObject((JSONObject) object));
            } else if (object.getClass().toString().endsWith("JSONArray")) {
                if (o1.getJSONArray(key) == null || o1.getJSONArray(key).size() == 0 || o1.getJSONArray(key).get(0).getClass().toString().endsWith("String")) {
                    o2.put(key.toLowerCase(), object);
                }
                // o2.put(key.toLowerCase(), toLowerKeyArray((JSONArray)o1.getJSONArray(key)));
                o2.put(key.toLowerCase(), toLowerKeyArray(o1.getJSONArray(key)));
            } else {
                try {
                    object = JSONObject.parseObject(object.toString());
                    //存在字符串对象无法识别的情况,需手动转换一下
                    o2.put(key.toLowerCase(), toLowerKeyObject((JSONObject) object));
                } catch (Exception e) {
                    o2.put(key.toLowerCase(), object);
                }

            }
        }
        return o2;
    }

    //数组key小写
    public JSONArray toLowerKeyArray(JSONArray o1) {
        JSONArray o2 = new JSONArray();
        for (int i = 0; i < o1.toArray().length; i++) {
            Object jArray = o1.get(i);
            if (jArray.getClass().toString().endsWith("JSONObject")) {
                //对象,调用对象key小写方法
                o2.add(toLowerKeyObject((JSONObject) jArray));
            } else if (jArray.getClass().toString().endsWith("JSONArray")) {
                //数组,递归,继续
                o2.add(toLowerKeyArray((JSONArray) jArray));
            } else {
                try {
                    jArray = JSONObject.parseObject(jArray.toString());
                    //存在字符串对象无法识别的情况,需手动转换一下
                    o2.add(toLowerKeyObject((JSONObject) jArray));
                } catch (Exception e) {
                    //字符串,无需转为小写
                    return o1;
                }

            }

        }
        return o2;
    }

    //获取map json串指定key的value值
    public String getMapValue(String jsonStr, String key) {
        try {
            //匹配key,忽略key大小写
            String[] keys = key.split("\\.");
            int layersize = keys.length;
            String mainKey = keys[layersize - 1];
            String regex = "(?i)" + mainKey + "\\s*" + "=";
            String returnStr = null;
            //获取当前对象指定层级的对象
            for (String str : getLayerMatchPair(jsonStr, '{', '}', layersize - 1)) {
                String params[] = str.split(regex);
                //未包含key
                if (params.length <= 1) {
                    continue;
                } else if (params.length == 2) {
                    //只存在一个key
                    if (params[1].trim().startsWith("{")) {
                        //key值是对象,返回对象
                        returnStr = getMatchPair(params[1].trim(), '{', '}', 0);
                        break;
                    }
                    //value值为字符串,逗号截取,字段未最后一个,需要清除 }
                    //key为字符串,返回字符串
                    returnStr = params[1].trim().split(",")[0].replace("}", "").trim();
                    break;

                } else {
                    //存在多个key,只取最外层的
                    str = str.trim().substring(1, str.length() - 1);
                    //取下一层,看是否包含key,包含则剔除
                    for (String endStr : getLayerMatchPair(str, '{', '}', 0)) {
                        //包含key,且对象外往前推的字符串只存在一个key,剔除,防止 a:{a:value}的情况
                        if (endStr.split(regex).length > 1 && str.substring(str.indexOf(endStr) - key.length() - 2, str.indexOf(endStr) + endStr.length()).split(regex).length == 2) {
                            str = str.replace(endStr, "");
                        }
                    }
                    //替换过后依旧存在相同的key,则为key.key的情况
                    if (str.split(regex).length > 2) {
                        for (String endStr : getLayerMatchPair(str, '{', '}', 0)) {
                            //包含key,且只包含一个key，剔除
                            if (endStr.split(regex).length == 2) {
                                return endStr;
                            }
                        }
                    }

                    params = str.split(regex);
                    if (params[1].trim().startsWith("{")) {
                        //key值是对象,返回对象
                        return getMatchPair(params[1].trim(), '{', '}', 0);
                    }
                    //value值为字符串,逗号截取,字段未最后一个,需要清除 }
                    //key为字符串,返回字符串
                    return params[1].trim().split(",")[0].replace("}", "").trim();
                }

            }
            return returnStr;
        } catch (Exception e) {
            return null;
        }

    }

    //获取指定匹配的内容
    //layerIndex 获取嵌套的第几层,从0开始,
    public List<String> getLayerMatchPair(String src, char start, char end, int layerIndex) {

        int i = 0;
        List<String> lists = new ArrayList<>();
        while (getMatchPair(src, start, end, i) != null) {
            lists.add(getMatchPair(src, start, end, i));
            i = i + 1;
        }
        if (layerIndex == 0) {
            return lists;
        }
        List<String> lists2 = new ArrayList<>();
        for (String list : lists) {
            for (String l : getLayerMatchPair(list.trim().substring(1, list.length() - 1), start, end, layerIndex - 1)) {
                lists2.add(l);
            }
        }
        return lists2;

    }

    //找到成对的指定字符包含的内容
    //index获取第几对,从0开始
    public String getMatchPair(String src, char start, char end, int index) {
        int number = src.length();
        index = index + 1;
        //成对个数
        int pair = getOccurCount(src, String.valueOf(start));
        //参数校验,不成对 或 成对个数小于获取第几对的值
        if (number < index || pair < index) {
            return null;
        }
        int startIndex = src.indexOf(start);
        int endIndex = 0;
        int startNum = 1;
        int endNum = 0;
        int pariNum = 0;
        for (int i = startIndex + 1; i < src.length(); i++) {
            //开始字符匹配,加一
            if (src.charAt(i) == start) {
                //开始=结束,已经成对,修改开始的索引值,获取下一对的开始
                if (startNum == endNum) {
                    startIndex = i;
                }
                startNum = startNum + 1;
            } else if (src.charAt(i) == end) {
                //结束字符匹配,加一
                endNum = endNum + 1;
                //开始字符数量=结束字符数量,已经成对,成对值+1
                if (startNum == endNum) {
                    pariNum = pariNum + 1;
                    //成对数量=需要获取的第几对,返回字符所在的索引值
                    if (pariNum == index) {
                        endIndex = i;
                        break;
                    }

                }
            }

        }
        if (endIndex == 0) {
            return null;
        }

        return src.substring(startIndex, endIndex + 1);

    }

    //获取指定字符串出现个数
    public int getOccurCount(String src, String find) {
        int o = 0;
        int index = -1;
        while ((index = src.indexOf(find, index)) > -1) {
            ++index;
            ++o;
        }
        return o;
    }


    /**
     * @param jsonStr 传入的参数字符串
     * @return return an interger
     * 1:json {k1:v1, k2:v2, ...}
     * 2:数组  [[], [], [], ...]   [{k:v}, {k:v}, ...]
     * 3:map  {k1=v1, k2=v2, ...}    [{k1=v1, k2=v2, ...}, {k1=v1, k2=v2, ...}, ...]
     */
    public int getJosnType(String jsonStr) {
        try {
            if (jsonStr.matches(objectRegex)) { // 符合1 json {k1:v1, k2:v2, ...}, 3 map {k1=v1, k2=v2, ...}
                try {
                    //解析成功为 json 对象，如果解析ok,即这一步无异常，则返回1，代表输入的字符串形如{k1:v1, k2:v2, ...}
                    JSONObject.parseObject(jsonStr);
                    return 1;
                } catch (Exception ee) {  // {k1=v1, k2=v2, ...}，返回3 map
                    //解析失败,并且包含=,map对象;{k1=v1, k2=v2, ...}
                    if (jsonStr.contains("=")) {
                        return 3;
                    }
                    //无法识别的字符串类型
                    return 0;
                }
            } else if (jsonStr.matches(arrayRegex)) {
                // 如果满足数组的匹配，[[], [], ...]  [{k:v}, {k:v}, ...] [{k1=v1, k2=v2, ...}, {k1=v1, k2=v2, ...}, ...]
                try {
                    //解析成功为json数组；[[], [], ...]  [{k:v}, {k:v}, ...] 返回2
                    JSONArray.parseArray(jsonStr);
                    return 2;
                } catch (Exception ee) {
                    //解析失败,并且包含=,map对象,[{k1=v1, k2=v2, ...}, {k1=v1, k2=v2, ...}, ...] 返回3
                    if (jsonStr.contains("=")) {
                        return 3;
                    }
                    return 2;
                }
            } else {
                return 0;
            }
        } catch (Exception e) {
            return 0;
        }
    }


    public static void main(String[] args) {
        /**
         * 1： [n1, n2, n3, n4, ...]              数组
         * 2： {k1:v1, k2:{K3:v3, ...}, ...}      字典, 含字典的值也是字典
         * 3： {k1=v1, k2={k3=v3, ...}, ...}      map, 含map的值也是map
         * 4： [[n1, n2, ...], [], [], ...]       数组里面是数组
         * 5： [{k:v}, {k1:{k2:v2}, ...}, ...]    数组里面是字典
         * 6:  [{k1=v1, ...}, {k2={k3=v3}}, ...]  数组里面是map
         *
         * PS: 数组类型里的{}外不需要引号，{}里面是map，key,value都不要引号；{}是字典：key需要引号，字符串类型的value需要引号
         */

        GetJsonValue getJsonValue = new GetJsonValue();

        // 测试1
        String arrayjson1 = "[1, 2, 3]";
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(arrayjson1), 1).toStringUtf8());  // 2 测试通过

        // 测试2
        String jsonStr2 = "{\"_id\" : \"586=;,e2e\", \"bizId\" : \"586d5\", \"toUserId\" : \"10347\", \"amount\" : \"0.01\", \"message\" : \"阳光普照1\", \"red_id\" : \"95161003011997696\", \"materialId\" : \"39\", \"redSendSuccess\" : true, \"msgSendSuccess\" : true, \"result\" : \"{\\\"code\\\":\\\"0000\\\",\\\"result\\\":{\\\"result\\\":\\\"0.01\\\",\\\"Count\\\":1,\\\"GroupId\\\":\\\"\\\",\\\"ID\\\":\\\"95161003011997696\\\",\\\"Message\\\":\\\"阳光普照1\\\",\\\"Recipient\\\":\\\"10347\\\"},\\\"message\\\":\\\"操作成功\\\",\\\"request_id\\\":\\\"cc724bc0038d49a59a4b1959ec45972a\\\"}\", \"createDate\" : \"2017-01-04T23:56:56.000+0000\"}";
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(jsonStr2), Slices.utf8Slice("ToUserId")).toStringUtf8()); // 10347 测试通过
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(jsonStr2), Slices.utf8Slice("result.resulT.id")).toStringUtf8());  // 95161003011997696 测试通过

        // 测试3
        String mapjson3 = "{type=100, file={sizeStr=238 KB, suffix=pdf, file_id=o11, type={type={aaa=bbb}}, file_name=“儿童晕厥订版)”解（王成，2016）.pdf, size=2491, file_url=hp://cy.f.dom.cn/o_1btf2b8f6li72gq16olnjdkhn11,xxx={yyy=zzz}}}";
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(mapjson3), Slices.utf8Slice("type")).toStringUtf8());  // 100 测试通过
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(mapjson3), Slices.utf8Slice("file.type.type")).toStringUtf8());  // {aaa=bbb} 测试通过
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(mapjson3), Slices.utf8Slice("file.type.type.aaa")).toStringUtf8());  // bbb 测试通过

        // 测试4
        String arrayjson4 = "[[aaa,111],[bbb,222]]";
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(arrayjson4), 0).toStringUtf8());  // [aaa,111] 测试通过

        // 测试5
        String jsonStr5 = "[{\"nAme\":\"yang\",\"age\":9,\"addr\":{\"country\":\"中国\",\"city\":\"深圳\",\"compaNy\":[\"大辰\",\"玄关\"]}},{\"nAme\":\"LI\",\"age\":9,\"addr\":{\"country\":\"CHINAME\",\"city\":\"深圳\",\"compaNy\":[\"大辰2\",\"玄关2\"]}}]";
        String key1 = "nAme";
        Slice res1 = getJsonValue.getJsonValue(Slices.utf8Slice(jsonStr5), 1, Slices.utf8Slice(key1));
        System.out.println(res1.toStringUtf8());  // LI 测试通过
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(jsonStr5), 0, Slices.utf8Slice("addr.city")).toStringUtf8());  // 深圳 测试通过

        // 测试6
        String arrayjson6 = "[{name={name=12456}, age=100, sex=M},{name=mary, age=99}]";
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(arrayjson6), 0, Slices.utf8Slice("name.Name")).toStringUtf8());  // 12456 测试通过

        // 测试7 特殊符号测试
        String arrayjson8 = "{\n" + "  \"courseId\" : \"619493640888127488\"\n" + "}";
        System.out.println(getJsonValue.getJsonValue(Slices.utf8Slice(arrayjson8), Slices.utf8Slice("couRseId")).toStringUtf8()); // 619493640888127488 测试通过

    }

}
