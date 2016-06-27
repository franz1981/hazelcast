package com.hazelcast.spi.impl.operationexecutor.impl;

public final class PartitionGroupMappers {

   private PartitionGroupMappers() {
   }

   public static PartitionOperationThread.PartitionGroupMapper maskWith(final int groupMask) {
      return new PartitionOperationThread.PartitionGroupMapper() {
         @Override
         public int groupIdOf(int partitionId) {
            return partitionId & groupMask;
         }
      };
   }

}
