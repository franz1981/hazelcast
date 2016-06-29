/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An {@link OperationThread} that executes Operations for a particular partition, e.g. a map.get operation.
 */
public final class PartitionOperationThread extends OperationThread {

   private final OperationRunner[] partitionOperationRunners;
   private final GroupLock groupLock;
   private final WaitStrategy waitStrategy;
   private final PartitionOperationRunnerEventListener partitionOperationRunnerEventListener;

   @SuppressFBWarnings("EI_EXPOSE_REP")
   public PartitionOperationThread(String name,
                                   int threadId,
                                   OperationQueue queue,
                                   ILogger logger,
                                   HazelcastThreadGroup threadGroup,
                                   NodeExtension nodeExtension,
                                   OperationRunner[] partitionOperationRunners,
                                   WaitStrategy waitStrategy,
                                   GroupLock groupLock,
                                   PartitionOperationRunnerEventListener partitionOperationRunnerEventListener) {
      super(name, threadId, queue, logger, threadGroup, nodeExtension, false);
      this.partitionOperationRunnerEventListener = partitionOperationRunnerEventListener;
      this.partitionOperationRunners = partitionOperationRunners;
      this.groupLock = groupLock;
      this.waitStrategy = waitStrategy;
   }

   /**
    * For each partition there is a {@link com.hazelcast.spi.impl.operationexecutor.OperationRunner} instance. So we need to
    * find the right one based on the partition-id.
    */
   @Override
   public OperationRunner getOperationRunner(int partitionId) {
      return partitionOperationRunners[partitionId];
   }

   protected final void runWithCurrentRunner(final Packet packet) throws Exception {
      final int partitionId = packet.getPartitionId();
      int idleCounter = 0;
      boolean run = false;
      while (!isInterrupted() && !run) {
         if (!groupLock.tryLock(partitionId)) {
            idleCounter = waitStrategy.idle(idleCounter);
         }
         else {
            partitionOperationRunnerEventListener.beforeRunWith(currentRunner);
            try {
               super.runWithCurrentRunner(packet);
            }
            finally {
               partitionOperationRunnerEventListener.finishedRunWith(currentRunner);
               groupLock.unlock(partitionId);
               idleCounter = 0;
               run = true;
            }
         }
      }
      if (!run) {
         throw new IllegalStateException("interrupted while performing a spin lock!");
      }
   }

   protected final void runWithCurrentRunner(final Operation operation) {
      final int partitionId = operation.getPartitionId();
      int idleCounter = 0;
      boolean run = false;
      while (!isInterrupted() && !run) {
         if (!groupLock.tryLock(partitionId)) {
            idleCounter = waitStrategy.idle(idleCounter);
         }
         else {
            partitionOperationRunnerEventListener.beforeRunWith(currentRunner);
            try {
               super.runWithCurrentRunner(operation);
            }
            finally {
               partitionOperationRunnerEventListener.finishedRunWith(currentRunner);
               groupLock.unlock(partitionId);
               idleCounter = 0;
               run = true;
            }
         }
      }
      if (!run) {
         throw new IllegalStateException("interrupted while performing a spin lock!");
      }

   }

   protected final void runWithCurrentRunner(final PartitionSpecificRunnable partitionRunnable) {
      final int partitionId = partitionRunnable.getPartitionId();
      int idleCounter = 0;
      boolean run = false;
      while (!isInterrupted() && !run) {
         if (!groupLock.tryLock(partitionId)) {
            idleCounter = waitStrategy.idle(idleCounter);
         }
         else {
            partitionOperationRunnerEventListener.beforeRunWith(currentRunner);
            try {
               super.runWithCurrentRunner(partitionRunnable);
            }
            finally {
               partitionOperationRunnerEventListener.finishedRunWith(currentRunner);
               groupLock.unlock(partitionId);
               idleCounter = 0;
               run = true;
            }
         }
      }
      if (!run) {
         throw new IllegalStateException("interrupted while performing a spin lock!");
      }
   }

   @Probe
   int priorityPendingCount() {
      return queue.prioritySize();
   }

   @Probe
   int normalPendingCount() {
      return queue.normalSize();
   }

   public static interface PartitionGroupMapper {

      int groupIdOf(int partitionId);
   }

   public static interface WaitStrategy {

      int idle(int idleCounter);
   }
}
