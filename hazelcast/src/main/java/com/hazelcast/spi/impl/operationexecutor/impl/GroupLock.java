package com.hazelcast.spi.impl.operationexecutor.impl;


public final class GroupLock {

   private final SparsePaddedAtomicIntegerArray groupLocks;

   public GroupLock(final int groups) {
      this.groupLocks = new SparsePaddedAtomicIntegerArray(groups);
   }

   public final boolean isAcquired(final int index) {
      return groupLocks.get(index) > 0;
   }

   public final boolean isReleased(final int index) {
      return groupLocks.get(index) == 0;
   }

   public final int groups() {
      return this.groupLocks.length();
   }

   public final boolean tryLock(final int index) {
      return (groupLocks.getAndIncrement(index) == 0);
   }

   public final void unlock(final int index) {
      this.groupLocks.set(index, 0);
   }
}
