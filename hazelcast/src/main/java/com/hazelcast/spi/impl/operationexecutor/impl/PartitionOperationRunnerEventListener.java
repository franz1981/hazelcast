package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

public interface PartitionOperationRunnerEventListener {
   void beforeRunWith(OperationRunner partitionOperationRunner);
   void finishedRunWith(OperationRunner partitionOperationRunner);
}
