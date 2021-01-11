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
import java.util.Set;

/**
 * 返回当月月份第一天的日期
 */

public final class MonthBegin {

    @ScalarFunction("month_begin")  // 函数名
    @SqlType(StandardTypes.VARCHAR)
    @Description("Returns the day from timestamp")
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
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        output = day.format(calendar.getTime());
        return Slices.utf8Slice(output);
    }

    @ScalarFunction("month_begin")
    @SqlType(StandardTypes.VARCHAR)  // 函数的结果返回类型，为 VARCHAR 型字符串
    @Description("Returns the day from timestamp")
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
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        output = day.format(calendar.getTime());
        return Slices.utf8Slice(output);
    }


    public static void main(String[] args) {
        // 测试通过
//         System.out.println(MonthBegin.timestampToDay(Slices.utf8Slice(args[0].toString())).toStringUtf8());
//         System.out.println(MonthBegin.timestampToDay(20201109L).toStringUtf8());
//         System.out.println(MonthBegin.timestampToDay(Long.parseLong(args[0])).toStringUtf8());

    }
}
