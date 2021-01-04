package com.doordash.pragmatic_primitives.helper;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public abstract class BaseHelperTest {
    @Parameterized.Parameter
    public BaseHelper helper;

    @Parameterized.Parameters
    public static Collection<BaseHelper> getHelpers() {
        return Arrays.asList(OriginalHelper.INSTANCE, WorkStealingHelper.INSTANCE);
    }
}
