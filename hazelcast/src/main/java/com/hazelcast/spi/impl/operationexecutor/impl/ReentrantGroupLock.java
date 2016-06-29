package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.util.MutableInteger;

public final class ReentrantGroupLock {

   private final SparsePaddedAtomicIntegerArray groupLocks;
   //TODO preferred byte array counter to reduce memory footprint -> BUT use unsigned bytes
   private final ThreadLocal<byte[]> groupAccesses;
   private final ThreadLocal<MutableInteger> groupAccessesCount;

   public ReentrantGroupLock(final int groups) {
      this.groupAccesses = new ThreadLocal<byte[]>();
      this.groupLocks = new SparsePaddedAtomicIntegerArray(groups);
      this.groupAccessesCount = new ThreadLocal<MutableInteger>();
   }

   public final boolean isAcquired(final int index) {
      return groupLocks.get(index) > 0;
   }

   public final boolean isReleased(final int index) {
      return groupLocks.get(index) == 0;
   }

   public final int groupAccesses(final int index) {
      final byte[] threadGroupAccesses = this.groupAccesses.get();
      if (threadGroupAccesses == null)
         return 0;
      final int groupAccessCount = threadGroupAccesses[index] & 255;
      return groupAccessCount;
   }

   public final int groupAccessesCount() {
      final MutableInteger count = groupAccessesCount.get();
      if (count == null)
         return 0;
      return count.value;
   }

   public final int groups() {
      return this.groupLocks.length();
   }

   private boolean tryLockFirstTime(final int index){
      //TODO MEASURE!! try to perform a wait-free (if supported) atomic increment -> scale better when contended?
      //it succeed only if is the first different thread that try to enter
      if (groupLocks.getAndIncrement(index) == 0) {

         final byte[] threadGroupAccesses = new byte[groupLocks.length()];
         threadGroupAccesses[index] = 1;
         this.groupAccesses.set(threadGroupAccesses);

         final MutableInteger groupAccessTotalCount = new MutableInteger();
         groupAccessTotalCount.value = 1;
         this.groupAccessesCount.set(groupAccessTotalCount);

         return true;
      }
      else {
         return false;
      }
   }

   private final boolean tryReentrantLock(final int index, final int groupAccessCount, final byte[] threadGroupAccesses, final MutableInteger groupAccessTotalCount){
      final int nextGroupAccessCount = groupAccessCount + 1;
      if (nextGroupAccessCount > 255) {
         throw new IllegalStateException("reached the maximum reentrant count!");
      }
      threadGroupAccesses[index] = (byte) nextGroupAccessCount;
      groupAccessTotalCount.value++;
      return true;
   }

   public final boolean tryLock(final int index) {
      byte[] threadGroupAccesses = this.groupAccesses.get();
      MutableInteger groupAccessTotalCount = this.groupAccessesCount.get();
      if (threadGroupAccesses != null) {
         final int groupAccessCount = threadGroupAccesses[index] & 255;
         if (groupAccessCount > 0) {
            tryReentrantLock(index,groupAccessCount,threadGroupAccesses,groupAccessTotalCount);
            final int nextGroupAccessCount = groupAccessCount + 1;
            if (nextGroupAccessCount > 255) {
               throw new IllegalStateException("reached the maximum reentrant count!");
            }
            threadGroupAccesses[index] = (byte) nextGroupAccessCount;
            groupAccessTotalCount.value++;
            return true;
         }
         else {
            //TODO MEASURE!! try to perform a wait-free (if supported) atomic increment -> scale better when contended?
            //it succeed only if is the first different thread that try to enter
            if (groupLocks.getAndIncrement(index) == 0) {
               threadGroupAccesses[index] = 1;
               groupAccessTotalCount.value++;
               return true;
            }
            else {
               return false;
            }
         }
      }
      else {
         return tryLockFirstTime(index);
      }
   }

   public final void unlock(final int index) {
      byte[] threadGroupAccesses = this.groupAccesses.get();
      if (threadGroupAccesses == null) {
         throw new IllegalStateException("can't unlock without a successfull tryLock first!");
      }
      final int groupAccessCount = threadGroupAccesses[index] & 255;
      if (groupAccessCount > 0) {
         final int nextGroupAccessCount = groupAccessCount - 1;
         threadGroupAccesses[index] = (byte) nextGroupAccessCount;
         final MutableInteger totalAccessesCount = this.groupAccessesCount.get();
         totalAccessesCount.value--;
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
