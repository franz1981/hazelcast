package com.hazelcast.spi.impl.operationexecutor.impl;

final class ReentrantGroupLock {

   private final SparsePaddedAtomicIntegerArray groupLocks;
   //TODO preferred byte array counter to reduce memory footprint -> BUT use unsigned bytes
   private final ThreadLocal<byte[]> groupAccesses;

   public ReentrantGroupLock(final int groups) {
      this.groupAccesses = new ThreadLocal<byte[]>() {
         @Override
         protected byte[] initialValue() {
            return new byte[groups];
         }
      };
      this.groupLocks = new SparsePaddedAtomicIntegerArray(groups);
   }

   public final boolean isAcquired(final int index){
      return groupLocks.get(index)>0;
   }

   public final boolean isReleased(final int index){
      return groupLocks.get(index)==0;
   }

   public final int groupAccesses(final int index) {
      final byte[] threadGroupAccesses = this.groupAccesses.get();
      final int groupAccessCount = threadGroupAccesses[index] & 255;
      return groupAccessCount;
   }

   public final int groups() {
      return this.groupLocks.length();
   }

   public final boolean tryLock(final int index) {
      final byte[] threadGroupAccesses = this.groupAccesses.get();
      final int groupAccessCount = threadGroupAccesses[index] & 255;
      if (groupAccessCount > 0) {
         final int nextGroupAccessCount = groupAccessCount + 1;
         if (nextGroupAccessCount > 255) {
            throw new IllegalStateException("reached the maximum reentrant count!");
         }
         threadGroupAccesses[index] = (byte) nextGroupAccessCount;
         return true;
      }
      else {
         //TODO MEASURE!! try to perform a wait-free (if supported) atomic increment -> scale better when contended?
         //it succeed only if is the first different thread that try to enter
         if (groupLocks.getAndIncrement(index) == 0) {
            threadGroupAccesses[index] = 1;
            return true;
         }
         else {
            return false;
         }
      }
   }

   public final void unlock(final int index) {
      final byte[] threadGroupAccesses = this.groupAccesses.get();
      final int groupAccessCount = threadGroupAccesses[index] & 255;
      if (groupAccessCount > 0) {
         final int nextGroupAccessCount = groupAccessCount - 1;
         threadGroupAccesses[index] = (byte) nextGroupAccessCount;
         //release only if is the last access to be removed
         if (nextGroupAccessCount == 0) {
            //ATTENTION!! do not use a plain/ordered store -> by lock's contract loads can't escape the mutual esclusion region!
            this.groupLocks.set(index, 0);
         }
      }
      else {
         throw new IllegalStateException("can't unlock without a successfull tryLock first!");
      }
   }

}
