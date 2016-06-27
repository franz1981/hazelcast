package com.hazelcast.spi.impl.operationexecutor.impl;

import java.util.concurrent.locks.LockSupport;

public enum WaitStrategies implements PartitionOperationThread.WaitStrategy {
   Sleeping {
      @Override
      public int idle(int idleCounter) {
         if (idleCounter < 100) {
            //NOOP
            return idleCounter + 1;
         }
         if (idleCounter < 200) {
            Thread.yield();
            return idleCounter + 1;
         }
         else {
            LockSupport.parkNanos(1L);
            return idleCounter;
         }
      }
   }
}
