package com.to8to.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * 返回当周，前后几周的第一天的日期
 * 根据传参的个数决定
 */

public final class WeekBegin {
    /*
    @ScalarFunction 定义了函数名
    @SqlType 里表明了函数的返回类型
    @Description 是函数的描述
    输入的参数的是Long型的数字
     */
    @ScalarFunction("week2_begin")
    @SqlType(StandardTypes.VARCHAR)
    @Description("返回日期当周的第一天（周日）的日期 input:yyyyMMdd")
    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.BIGINT) Long time) {
        Date date = null;
        String output = "";
        SimpleDateFormat day = new SimpleDateFormat("yyyyMMdd");
        try {
            if (time == null || time.toString().length() < 8) {
                return Slices.utf8Slice(output);
            } else if (time.toString().length() == 8 && time >= 19700101 && time <= 21000101) {
                date = new Date(day.parse(time.toString()).getTime());
            }
        } catch (Exception e) {
            return Slices.utf8Slice(output);
        }
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        int i = calendar.get(Calendar.DAY_OF_WEEK);  // 一周的第几天
        calendar.add(Calendar.DATE,-(i-1));
        output = day.format(calendar.getTime());
        return Slices.utf8Slice(output);
    }

    /*
    输入的参数是 VARCHAR参数，参数的类型是Slice，VARCHAR使用Slice，本质上是的包装byte[]，而不是String其本机容器类型
     */
    @ScalarFunction("week2_begin")
    @SqlType(StandardTypes.VARCHAR)
    @Description("返回日期当周的第一天（周日）的日期 input:yyyyMMdd或者yyyy-MM-dd")
    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice times) {
        String output = "";
        Date date = null;
        SimpleDateFormat day = new SimpleDateFormat("yyyyMMdd");
        try {
            String time = times.toStringUtf8();
            if (time == null || (time.length() != 8 && time.length() != 10)) {
                return Slices.utf8Slice(output);
            } else if (time.contains("-") && time.length() == 10) {
                try {
                    time = time.replace("-","");
                } catch (Exception e) {
                    return Slices.utf8Slice(output);
                }
            }

            if (time.length() == 8 && Long.parseLong(time) >= 19700101 && Long.parseLong(time) <= 21000101) {
                time = String.valueOf(day.parse(time).getTime());
                String d = day.format(new Date(Long.parseLong(time))); // 时间戳转换日期
                date = day.parse(d);
            }
        } catch (Exception e) {
            return Slices.utf8Slice(output);
        }
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        int i = calendar.get(Calendar.DAY_OF_WEEK);  // 一周的第几天
        calendar.add(Calendar.DATE,-(i-1));
        output = day.format(calendar.getTime());
        return Slices.utf8Slice(output);
    }

    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice times, int num) {
        String output = "";
        Date date = null;
        SimpleDateFormat day = new SimpleDateFormat("yyyyMMdd");
        try {
            String time = times.toStringUtf8();
            if (time == null || (time.length() != 8 && time.length() != 10)) {
                return Slices.utf8Slice(output);
            } else if (time.contains("-") && time.length() == 10) {
                try {
                    time = time.replace("-","");
                } catch (Exception e) {
                    return Slices.utf8Slice(output);
                }
            }

            if (time.length() == 8 && Long.parseLong(time) >= 19700101 && Long.parseLong(time) <= 21000101) {
                time = String.valueOf(day.parse(time).getTime());
                String d = day.format(new Date(Long.parseLong(time))); // 时间戳转换日期
                date = day.parse(d);
            }
        } catch (Exception e) {
            return Slices.utf8Slice(output);
        }
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        int i = calendar.get(Calendar.DAY_OF_WEEK);  // 一周的第几天
        calendar.add(Calendar.DATE,-(i-1)+7*num);
        output = day.format(calendar.getTime());
        return Slices.utf8Slice(output);
    }

    public static void main(String[] args) {
        if (args.length==2){
            System.out.println(WeekBegin.timestampToDay(Slices.utf8Slice(args[0].toString()), Integer.parseInt(args[1].toString())).toStringUtf8());
        }else {
            System.out.println(WeekBegin.timestampToDay(Slices.utf8Slice(args[0].toString())).toStringUtf8());
        }
        // System.out.println(WeekBegin.timestampToDay(20201109L).toStringUtf8());
        // System.out.println(WeekBegin.timestampToDay(Long.parseLong(args[0])).toStringUtf8());


    }
}
