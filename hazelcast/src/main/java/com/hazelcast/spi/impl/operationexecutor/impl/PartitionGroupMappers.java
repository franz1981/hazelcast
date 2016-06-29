package com.hazelcast.spi.impl.operationexecutor.impl;

public final class PartitionGroupMappers {

   private static final PartitionOperationThread.PartitionGroupMapper IDENTITY = new PartitionOperationThread.PartitionGroupMapper() {
      @Override
      public int groupIdOf(int partitionId) {
         return partitionId;
      }
   };

   private PartitionGroupMappers() {
   }

   static PartitionOperationThread.PartitionGroupMapper maskWith(final int groups) {
      return new PartitionOperationThread.PartitionGroupMapper() {

         private final int groupMask = groups-1;

         @Override
         public int groupIdOf(int partitionId) {
            return partitionId & groupMask;
         }
      };
   }

   static PartitionOperationThread.PartitionGroupMapper identity(){
      return IDENTITY;
   }

   static PartitionOperationThread.PartitionGroupMapper mod(final int groups){
      return new PartitionOperationThread.PartitionGroupMapper() {

         @Override
         public int groupIdOf(int partitionId) {
            return partitionId%groups;
         }
      };
   }

   public static PartitionOperationThread.PartitionGroupMapper maps(final int groups,final int partitions){
      if(groups<=0||partitions<=0){
         throw new IllegalArgumentException("groups and partitions must be >0!");
      }
      if(groups>partitions)
         throw new IllegalArgumentException("groups can't be more than the partitions!");
      if(groups==partitions){
         return identity();
      }else if(isPowerOfTwo(groups)){
         return maskWith(groups);
      }
      return mod(groups);
   }

   private static boolean isPowerOfTwo(final int value) {
      return (value & (value - 1)) == 0;
   }

}
