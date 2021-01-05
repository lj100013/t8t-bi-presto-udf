package com.to8to.udf;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.Plugin;

import java.util.Set;

public class UdfPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(GetDay.class)
                .build();
    }
}