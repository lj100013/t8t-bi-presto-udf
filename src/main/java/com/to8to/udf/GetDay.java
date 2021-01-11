package com.to8to.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.text.SimpleDateFormat;
import java.util.Date;

public final class GetDay {
    @ScalarFunction("getday")
    @SqlType(StandardTypes.VARCHAR)
    @Description("Returns the day from timestamp")
    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.BIGINT) Long time) {

        String output = "";

        try {
            if (time == null || time.toString().length() < 10) {
                return Slices.utf8Slice(output);
            } else if (time.toString().length() == 8 && time >= 19700101 && time <= 21000101) {
                output = time.toString().substring(0, 4) + "-" + time.toString().substring(4, 6) + "-" + time.toString().substring(6, 8);
                return Slices.utf8Slice(output);
            }
            SimpleDateFormat day = new SimpleDateFormat("yyyyMMdd");
            String timestamp = time.toString();
            output = day.format(timestamp.length() == 10 ? new Date(Long.parseLong(timestamp) * 1000) : new Date(Long.parseLong(timestamp)));
        } catch (Exception e) {
        }
        return Slices.utf8Slice(output);
    }


    @ScalarFunction("getday")
    @SqlType(StandardTypes.VARCHAR)
    @Description("Returns the day from timestamp")
    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice times) {
        String output = "";
        try {
            String time = times.toStringUtf8();
            SimpleDateFormat day = new SimpleDateFormat("yyyyMMdd");
            if (time == null || time.length() < 10) {
                return Slices.utf8Slice(output);
            } else if (time.contains("-")) {
                try {
                    time = String.valueOf(day.parse(time).getTime());
                } catch (Exception e) {
                    return Slices.utf8Slice(output);
                }
            } else if (time.length() == 8 && Long.parseLong(time) >= 19700101 && Long.parseLong(time) <= 21000101) {
                output = time.substring(0, 4) + "-" + time.substring(4, 6) + "-" + time.substring(6, 8);
                return Slices.utf8Slice(output);
            }
            output = day.format(time.length() == 10 ? new Date(Long.parseLong(time) * 1000) : new Date(Long.parseLong(time)));
        } catch (Exception e) {

        }

        return Slices.utf8Slice(output);
    }

    @ScalarFunction("getday")
    @SqlType(StandardTypes.VARCHAR)
    @Description("Returns the day from timestamp")
    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice times, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice standard) {
        String output = "";
        try {
            String time = times.toStringUtf8();

            SimpleDateFormat day = new SimpleDateFormat(standard.toStringUtf8());
            if (time == null || time.length() < 10) {
                return Slices.utf8Slice(output);
            } else if (time.contains("-")) {
                try {
                    time = String.valueOf(day.parse(time).getTime());
                } catch (Exception e) {
                    return Slices.utf8Slice(output);
                }
            } else if (time.length() == 8 && Long.parseLong(time) >= 19700101 && Long.parseLong(time) <= 21000101) {
                output = time.substring(0, 4) + "-" + time.substring(4, 6) + "-" + time.substring(6, 8);
                return Slices.utf8Slice(output);
            }
            output = day.format(time.length() == 10 ? new Date(Long.parseLong(time) * 1000) : new Date(Long.parseLong(time)));
        } catch (Exception e) {
        }

        return Slices.utf8Slice(output);
    }

    @ScalarFunction("getday")
    @SqlType(StandardTypes.VARCHAR)
    @Description("Returns the day from timestamp")
    public static Slice timestampToDay(@SqlNullable @SqlType(StandardTypes.BIGINT) Long time, @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice standard) {
        String output = "";
        try {
            if (time == null || time.toString().length() < 10) {
                return Slices.utf8Slice(output);
            } else if (time.toString().length() == 8 && time >= 19700101 && time <= 21000101) {
                output = time.toString().substring(0, 4) + "-" + time.toString().substring(4, 6) + "-" + time.toString().substring(6, 8);
                return Slices.utf8Slice(output);
            }
            SimpleDateFormat day = new SimpleDateFormat(standard.toStringUtf8());
            String timestamp = time.toString();
            output = day.format(timestamp.length() == 10 ? new Date(Long.parseLong(timestamp) * 1000) : new Date(Long.parseLong(timestamp)));
        } catch (Exception e) {
        }

        return Slices.utf8Slice(output);
    }

    public static void main(String[] args) {

    }
}
