package com.daiichi;

import com.daiichi.pojo.ExecutionPlan;

public interface DBWrapper {
    void createTree(ExecutionPlan plan);

    void fetchAndUpdate(String planId);

    enum DB {
        ARGANO,
        ORIENT,
        NEPTUNE,
        NEO4J,
        AGE
    }
}
