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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.collection.MPSCQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationexecutor.OperationRunnerFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.properties.HazelcastProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.spi.properties.GroupProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_GROUP_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PRIORITY_GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link com.hazelcast.spi.impl.operationexecutor.OperationExecutor} that schedules:
 * <ol>
 * <li>partition specific operations to a specific partition-operation-thread (using a mod on the partition-id)</li>
 * <li>non specific operations to generic-operation-threads</li>
 * </ol>
 * The {@link #execute(Object, int, boolean)} accepts an Object instead of a runnable to prevent needing to
 * create wrapper runnables around tasks. This is done to reduce the amount of object litter and therefor
 * reduce pressure on the gc.
 * <p/>
 * There are 2 category of operation threads:
 * <ol>
 * <li>partition specific operation threads: these threads are responsible for executing e.g. a map.put.
 * Operations for the same partition, always end up in the same thread.
 * </li>
 * <li>
 * generic operation threads: these threads are responsible for executing operations that are not
 * specific to a partition. E.g. a heart beat.
 * </li>
 *
 * </ol>
 */
@SuppressWarnings("checkstyle:methodcount")
public final class OperationExecutorImpl implements OperationExecutor, MetricsProvider {

   private static final int TERMINATION_TIMEOUT_SECONDS = 3;

   private final ILogger logger;

   // all operations for specific partitions will be executed on these threads, e.g. map.put(key, value)
   private final PartitionOperationThread[] partitionThreads;
   private final OperationRunner[] partitionOperationRunners;
   private final OperationQueue genericQueue;
   // all operations that are not specific for a partition will be executed here, e.g. heartbeat or map.size()
   private final GenericOperationThread[] genericThreads;
   private final OperationRunner[] genericOperationRunners;

   private final Address thisAddress;
   private final OperationRunner adHocOperationRunner;
   private final int priorityThreadCount;

   private final ReentrantGroupLock groupLock;
   private final PartitionOperationThread.PartitionGroupMapper partitionGroupMapper;

   public OperationExecutorImpl(HazelcastProperties properties,
                                LoggingService loggerService,
                                Address thisAddress,
                                OperationRunnerFactory operationRunnerFactory,
                                HazelcastThreadGroup threadGroup,
                                NodeExtension nodeExtension) {
      this.genericQueue = new DefaultOperationQueue(new LinkedBlockingQueue<Object>(), new LinkedBlockingQueue<Object>());
      this.thisAddress = thisAddress;
      this.logger = loggerService.getLogger(OperationExecutorImpl.class);

      this.adHocOperationRunner = operationRunnerFactory.createAdHocRunner();
      this.partitionOperationRunners = initPartitionOperationRunners(properties, operationRunnerFactory);
      this.groupLock = initPartitionGroupLock(properties);
      this.partitionGroupMapper = PartitionGroupMappers.maps(this.groupLock.groups(),properties.getInteger(PARTITION_COUNT));
      this.partitionThreads = initPartitionThreads(properties, threadGroup, nodeExtension, WaitStrategies.Sleeping, partitionGroupMapper);

      this.priorityThreadCount = properties.getInteger(PRIORITY_GENERIC_OPERATION_THREAD_COUNT);
      this.genericOperationRunners = initGenericOperationRunners(properties, operationRunnerFactory);
      this.genericThreads = initGenericThreads(threadGroup, nodeExtension);
   }

   private static int getRunningOperationCount(OperationRunner[] runners) {
      int result = 0;
      for (OperationRunner runner : runners) {
         if (runner.currentTask() != null) {
            result++;
         }
      }
      return result;
   }

   private static void startAll(OperationThread[] operationThreads) {
      for (OperationThread thread : operationThreads) {
         thread.start();
      }
   }

   private static void shutdownAll(OperationThread[] operationThreads) {
      for (OperationThread thread : operationThreads) {
         thread.shutdown();
      }
   }

   private static void awaitTermination(OperationThread[] operationThreads) {
      for (OperationThread thread : operationThreads) {
         try {
            thread.awaitTermination(TERMINATION_TIMEOUT_SECONDS, SECONDS);
         }
         catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
         }
      }
   }

   private OperationRunner[] initPartitionOperationRunners(HazelcastProperties properties,
                                                           OperationRunnerFactory handlerFactory) {
      OperationRunner[] operationRunners = new OperationRunner[properties.getInteger(PARTITION_COUNT)];
      for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
         operationRunners[partitionId] = handlerFactory.createPartitionRunner(partitionId);
      }
      return operationRunners;
   }

   private OperationRunner[] initGenericOperationRunners(HazelcastProperties properties,
                                                         OperationRunnerFactory runnerFactory) {
      int threadCount = properties.getInteger(GENERIC_OPERATION_THREAD_COUNT);
      if (threadCount <= 0) {
         // default generic operation thread count
         int coreSize = Runtime.getRuntime().availableProcessors();
         threadCount = Math.max(2, coreSize / 2);
      }

      OperationRunner[] operationRunners = new OperationRunner[threadCount + priorityThreadCount];
      for (int partitionId = 0; partitionId < operationRunners.length; partitionId++) {
         operationRunners[partitionId] = runnerFactory.createGenericRunner();
      }

      return operationRunners;
   }

   private ReentrantGroupLock initPartitionGroupLock(HazelcastProperties properties) {
      int partitionGroupCount = properties.getInteger(PARTITION_GROUP_COUNT);
      System.out.println(partitionGroupCount);
      if (partitionGroupCount <= 0) {
         partitionGroupCount = properties.getInteger(PARTITION_COUNT);
      }
      return new ReentrantGroupLock(partitionGroupCount);
   }

   private PartitionOperationThread[] initPartitionThreads(HazelcastProperties properties,
                                                           HazelcastThreadGroup threadGroup,
                                                           NodeExtension nodeExtension,
                                                           PartitionOperationThread.WaitStrategy waitStrategy,
                                                           PartitionOperationThread.PartitionGroupMapper groupMapper) {
      int threadCount = properties.getInteger(PARTITION_OPERATION_THREAD_COUNT);
      if (threadCount <= 0) {
         // default partition operation thread count
         int coreSize = Runtime.getRuntime().availableProcessors();
         threadCount = Math.max(2, coreSize);
      }

      final PartitionOperationThread[] threads = new PartitionOperationThread[threadCount];
      for (int threadId = 0; threadId < threads.length; threadId++) {
         String threadName = threadGroup.getThreadPoolNamePrefix("partition-operation") + threadId;
         // the normalQueue will be a blocking queue. We don't want to idle, because there are many operation threads.
         MPSCQueue<Object> normalQueue = new MPSCQueue<Object>(null);
         OperationQueue operationQueue = new DefaultOperationQueue(normalQueue, new ConcurrentLinkedQueue<Object>());

         PartitionOperationThread partitionThread = new PartitionOperationThread(threadName, threadId, operationQueue, logger, threadGroup, nodeExtension, this.partitionOperationRunners, waitStrategy, this.groupLock, groupMapper);

         threads[threadId] = partitionThread;
         normalQueue.setConsumerThread(partitionThread);
      }

      // we need to assign the PartitionOperationThreads to all OperationRunners they own
      for (int partitionId = 0; partitionId < partitionOperationRunners.length; partitionId++) {
         int threadId = partitionId % threadCount;
         Thread thread = threads[threadId];
         OperationRunner runner = partitionOperationRunners[partitionId];
         runner.setCurrentThread(thread);
      }

      return threads;
   }

   private GenericOperationThread[] initGenericThreads(HazelcastThreadGroup threadGroup, NodeExtension nodeExtension) {
      // we created as many generic operation handlers, as there are generic threads
      int threadCount = genericOperationRunners.length;

      GenericOperationThread[] threads = new GenericOperationThread[threadCount];

      int threadId = 0;
      for (int threadIndex = 0; threadIndex < threads.length; threadIndex++) {
         boolean priority = threadIndex < priorityThreadCount;
         String baseName = priority ? "priority-generic-operation" : "generic-operation";
         String threadName = threadGroup.getThreadPoolNamePrefix(baseName) + threadId;
         OperationRunner operationRunner = genericOperationRunners[threadIndex];

         GenericOperationThread operationThread = new GenericOperationThread(threadName, threadIndex, genericQueue, logger, threadGroup, nodeExtension, operationRunner, priority);

         threads[threadIndex] = operationThread;
         operationRunner.setCurrentThread(operationThread);

         if (threadIndex == priorityThreadCount - 1) {
            threadId = 0;
         }
         else {
            threadId++;
         }
      }

      return threads;
   }

   @Override
   public void provideMetrics(MetricsRegistry metricsRegistry) {
      metricsRegistry.scanAndRegister(this, "operation");

      metricsRegistry.collectMetrics((Object[]) genericThreads);
      metricsRegistry.collectMetrics((Object[]) partitionThreads);
      metricsRegistry.collectMetrics(adHocOperationRunner);
      metricsRegistry.collectMetrics((Object[]) genericOperationRunners);
      metricsRegistry.collectMetrics((Object[]) partitionOperationRunners);
   }

   @SuppressFBWarnings("EI_EXPOSE_REP")
   @Override
   public OperationRunner[] getPartitionOperationRunners() {
      return partitionOperationRunners;
   }

   @SuppressFBWarnings("EI_EXPOSE_REP")
   @Override
   public OperationRunner[] getGenericOperationRunners() {
      return genericOperationRunners;
   }

   @Override
   public void scan(LiveOperations result) {
      scan(partitionOperationRunners, result);
      scan(genericOperationRunners, result);
   }

   private void scan(OperationRunner[] runners, LiveOperations result) {
      for (OperationRunner runner : runners) {
         Object task = runner.currentTask();
         if (!(task instanceof Operation) || task.getClass() == Backup.class) {
            continue;
         }
         Operation operation = (Operation) task;
         result.add(operation.getCallerAddress(), operation.getCallId());
      }
   }

   @Probe(name = "runningCount")
   @Override
   public int getRunningOperationCount() {
      return getRunningPartitionOperationCount() + getRunningGenericOperationCount();
   }

   @Probe(name = "runningPartitionCount")
   private int getRunningPartitionOperationCount() {
      return getRunningOperationCount(partitionOperationRunners);
   }

   @Probe(name = "runningGenericCount")
   private int getRunningGenericOperationCount() {
      return getRunningOperationCount(genericOperationRunners);
   }

   @Override
   @Probe(name = "queueSize", level = MANDATORY)
   public int getQueueSize() {
      int size = 0;
      for (PartitionOperationThread partitionThread : partitionThreads) {
         size += partitionThread.queue.normalSize();
      }
      size += genericQueue.normalSize();
      return size;
   }

   @Override
   @Probe(name = "priorityQueueSize", level = MANDATORY)
   public int getPriorityQueueSize() {
      int size = 0;
      for (PartitionOperationThread partitionThread : partitionThreads) {
         size += partitionThread.queue.prioritySize();
      }
      size += genericQueue.prioritySize();
      return size;
   }

   @Probe(name = "genericQueueSize")
   private int getGenericQueueSize() {
      return genericQueue.normalSize();
   }

   @Probe(name = "genericPriorityQueueSize")
   private int getGenericPriorityQueueSize() {
      return genericQueue.prioritySize();
   }

   @Override
   @Probe(name = "partitionThreadCount")
   public int getPartitionThreadCount() {
      return partitionThreads.length;
   }

   @Override
   @Probe(name = "genericThreadCount")
   public int getGenericThreadCount() {
      return genericThreads.length;
   }

   @Override
   public boolean isOperationThread() {
      return Thread.currentThread() instanceof OperationThread;
   }

   @Override
   public void execute(Operation op) {
      checkNotNull(op, "op can't be null");

      execute(op, op.getPartitionId(), op.isUrgent());
   }

   @Override
   public void execute(PartitionSpecificRunnable task) {
      checkNotNull(task, "task can't be null");

      execute(task, task.getPartitionId(), task instanceof UrgentSystemOperation);
   }

   @Override
   public void handle(Packet packet) {
      execute(packet, packet.getPartitionId(), packet.isUrgent());
   }

   private void execute(Object task, int partitionId, boolean priority) {
      if (partitionId < 0) {
         genericQueue.add(task, priority);
      }
      else {
         OperationThread partitionThread = partitionThreads[toPartitionThreadIndex(partitionId)];
         partitionThread.queue.add(task, priority);
      }
   }

   @Override
   public void executeOnPartitionThreads(Runnable task) {
      checkNotNull(task, "task can't be null");

      for (OperationThread partitionThread : partitionThreads) {
         partitionThread.queue.add(task, true);
      }
   }

   @Override
   public void interruptPartitionThreads() {
      for (PartitionOperationThread partitionThread : partitionThreads) {
         partitionThread.interrupt();
      }
   }

   @Override
   public void run(final Operation operation) {
      checkNotNull(operation, "operation can't be null");
      final Thread currentThread = Thread.currentThread();
      if (currentThread instanceof OperationHostileThread) {
         // OperationHostileThreads are not allowed to run any operation
         throw new IllegalThreadStateException("Can't call run from " + currentThread.getName() + " for:" + operation);
      }
      final int groupAccessesCount = this.groupLock.groupAccessesCount();
      //the count of the current thread groups locks acquired
      if (groupAccessesCount > 0) {
         //could be run only already acquired groups of partitions with specific partition operations -> avoid liveness failures
         final int operationPartitionId = operation.getPartitionId();
         if (operationPartitionId < 0) {
            throw new IllegalThreadStateException("Can't call run from " + currentThread.getName() + " for:" + operation);
         }
         else {
            final int operationGroupIndex = this.partitionGroupMapper.groupIdOf(operationPartitionId);
            //could require the reentrant lock only when the current thread already owns that partitions group
            if (this.groupLock.groupAccesses(operationGroupIndex) > 0) {
               final OperationRunner operationRunner = this.partitionOperationRunners[operationPartitionId];
               if (this.groupLock.tryLock(operationGroupIndex)) {
                  try {
                     operationRunner.run(operation);
                  }
                  finally {
                     this.groupLock.unlock(operationGroupIndex);
                  }
               }
               else {
                  throw new IllegalStateException("This state is IMPOSSIBLE...the lock is already acquired by the thread..");
               }
            }
            else {
               throw new IllegalThreadStateException("Can't call run from " + currentThread.getName() + " for:" + operation);
            }
         }
      }
      else {
         //there is no group locks acquired
         final int operationPartitionId = operation.getPartitionId();
         if (operationPartitionId < 0) {
            //user thread or partition thread without any group locks
            final OperationRunner operationRunner;
            //NO NEED TO CHECK AGAINST Partition Thread ->
            //a currentRunner exists only when there is a at least a partition group lock!
            if (!(currentThread instanceof GenericOperationThread)) {
               operationRunner = adHocOperationRunner;
            }
            else {
               //it can be either a Partition Thread running a Runnable or
               final GenericOperationThread operationThread = (GenericOperationThread) currentThread;
               operationRunner = operationThread.getOperationRunner(-1);
            }
            operationRunner.run(operation);
         }
         else {
            final int operationGroupIndex = this.partitionGroupMapper.groupIdOf(operationPartitionId);
            final OperationRunner operationRunner = this.partitionOperationRunners[operationPartitionId];
            if (this.groupLock.tryLock(operationGroupIndex)) {
               try {
                  operationRunner.run(operation);
               }
               finally {
                  this.groupLock.unlock(operationGroupIndex);
               }
            }
            else {
               throw new IllegalThreadStateException("can't acquire a lock on the operation's partition");
            }
         }
      }
   }

   OperationRunner getOperationRunner(Operation operation) {
      checkNotNull(operation, "operation can't be null");
      if (operation.getPartitionId() >= 0) {
         // retrieving an OperationRunner for a partition specific operation is easy; we can just use the partition id.
         return partitionOperationRunners[operation.getPartitionId()];
      }
      final Thread currentThread = Thread.currentThread();
      //user thread or partition thread without any group locks
      final OperationRunner operationRunner;
      //NO NEED TO CHECK AGAINST Partition Thread ->
      //a currentRunner exists only when there is a at least a partition group lock!
      if (!(currentThread instanceof GenericOperationThread)) {
         operationRunner = adHocOperationRunner;
      }
      else {
         //it can be either a Partition Thread running a Runnable or
         final GenericOperationThread operationThread = (GenericOperationThread) currentThread;
         operationRunner = operationThread.getOperationRunner(-1);
      }
      return operationRunner;
   }

   @Override
   public void runOrExecute(final Operation operation) {
      final Thread currentThread = Thread.currentThread();
      if (currentThread instanceof OperationHostileThread) {
         // OperationHostileThreads are not allowed to run any operation
         throw new IllegalThreadStateException("Can't call runOrExecute from " + currentThread.getName() + " for:" + operation);
      }
      final int groupAccessesCount = this.groupLock.groupAccessesCount();
      //the count of the current thread groups locks acquired
      if (groupAccessesCount > 0) {
         //could be run only already acquired groups of partitions with specific partition operations -> avoid liveness failures
         final int operationPartitionId = operation.getPartitionId();
         if (operationPartitionId < 0) {
            throw new IllegalThreadStateException("Can't call runOrExecute from " + currentThread.getName() + " for:" + operation);
         }
         else {
            final int operationGroupIndex = this.partitionGroupMapper.groupIdOf(operationPartitionId);
            //could require the reentrant lock only when the current thread already owns that partitions group
            if (this.groupLock.groupAccesses(operationGroupIndex) > 0) {
               final OperationRunner operationRunner = this.partitionOperationRunners[operationPartitionId];
               if (this.groupLock.tryLock(operationGroupIndex)) {
                  try {
                     operationRunner.run(operation);
                  }
                  finally {
                     this.groupLock.unlock(operationGroupIndex);
                  }
               }
               else {
                  throw new IllegalStateException("This state is IMPOSSIBLE...the lock is already acquired by the thread..");
               }
            }
            else {
               throw new IllegalThreadStateException("Can't call runOrExecute from " + currentThread.getName() + " for:" + operation);
            }
         }
      }
      else {
         //there is no group locks acquired
         final int operationPartitionId = operation.getPartitionId();
         if (operationPartitionId < 0) {
            //user thread or partition thread without any group locks
            final OperationRunner operationRunner;
            //NO NEED TO CHECK AGAINST Partition Thread ->
            //a currentRunner exists only when there is a at least a partition group lock!
            if (!(currentThread instanceof GenericOperationThread)) {
               operationRunner = adHocOperationRunner;
            }
            else {
               //it can be either a Partition Thread running a Runnable or
               final GenericOperationThread operationThread = (GenericOperationThread) currentThread;
               operationRunner = operationThread.getOperationRunner(-1);
            }
            operationRunner.run(operation);
         }
         else {
            final int operationGroupIndex = this.partitionGroupMapper.groupIdOf(operationPartitionId);
            final OperationRunner operationRunner = this.partitionOperationRunners[operationPartitionId];
            if (this.groupLock.tryLock(operationGroupIndex)) {
               try {
                  operationRunner.run(operation);
               }
               finally {
                  this.groupLock.unlock(operationGroupIndex);
               }
            }
            else {
               partitionThreads[toPartitionThreadIndex(operationPartitionId)].queue.add(operation, operation.isUrgent());
            }
         }
      }
   }

   @Override
   public boolean isRunAllowed(final Operation operation) {
      checkNotNull(operation, "operation can't be null");
      final Thread currentThread = Thread.currentThread();
      if (currentThread instanceof OperationHostileThread) {
         // OperationHostileThreads are not allowed to run any operation
         return false;
      }
      final int groupAccessesCount = this.groupLock.groupAccessesCount();
      //the count of the current thread groups locks acquired
      if (groupAccessesCount > 0) {
         //could be run only already acquired groups of partitions with specific partition operations -> avoid liveness failures
         final int operationPartitionId = operation.getPartitionId();
         if (operationPartitionId < 0) {
            return false;
         }
         else {
            final int operationGroupIndex = this.partitionGroupMapper.groupIdOf(operationPartitionId);
            //could require the reentrant lock only when the current thread already owns that partitions group
            if (this.groupLock.groupAccesses(operationGroupIndex) > 0) {
               return true;
            }
            else {
               return false;
            }
         }
      }
      else {
         //there is no group locks acquired
         final int operationPartitionId = operation.getPartitionId();
         if (operationPartitionId < 0) {
            return true;
         }
         else {
            final int operationGroupIndex = this.partitionGroupMapper.groupIdOf(operationPartitionId);
            //TODO too optimistic? a subsequent run could fail anyway due to contention with other threads
            return this.groupLock.isReleased(operationGroupIndex);
         }
      }
   }

   @Override
   public boolean isInvocationAllowed(Operation operation, boolean isAsync) {
      checkNotNull(operation, "operation can't be null");
      final Thread currentThread = Thread.currentThread();
      // IO threads are not allowed to run any operation
      if (currentThread instanceof OperationHostileThread) {
         return false;
      }
      // if it is async we don't need to check if it is PartitionOperationThread or not
      if (isAsync) {
         return true;
      }
      final int operationPartitionId = operation.getPartitionId();
      // allowed to invoke non partition specific task
      if (operationPartitionId < 0) {
         return true;
      }
      // allowed to invoke from non PartitionOperationThreads (including GenericOperationThread)
      if (currentThread.getClass() != PartitionOperationThread.class) {
         return true;
      }
      //exists any acquired group of partitions?
      if (groupLock.groupAccessesCount() > 0) {
         final int operationPartitionGroupId = this.partitionGroupMapper.groupIdOf(operationPartitionId);
         final int accessesPerPartitionGroupId = this.groupLock.groupAccesses(operationPartitionGroupId);
         //the acquired group of partitions contains the requested operation's partitionId?
         return accessesPerPartitionGroupId > 0;
      }
      else {
         //the operation is bounded correctly with its correct partition thread...
         final PartitionOperationThread partitionOperationThread = this.partitionThreads[toPartitionThreadIndex(operationPartitionId)];
         return (partitionOperationThread == currentThread);
      }
   }

   // public for testing purposes
   public int toPartitionThreadIndex(int partitionId) {
      return partitionId % partitionThreads.length;
   }

   @Override
   public void start() {
      logger.info("Starting " + partitionThreads.length + " partition threads");
      startAll(partitionThreads);

      logger.info("Starting " + genericThreads.length + " generic threads (" + priorityThreadCount + " dedicated for priority tasks)");
      startAll(genericThreads);
   }

   @Override
   public void shutdown() {
      shutdownAll(partitionThreads);
      shutdownAll(genericThreads);
      awaitTermination(partitionThreads);
      awaitTermination(genericThreads);
   }

   @Override
   public String toString() {
      return "OperationExecutorImpl{node=" + thisAddress + '}';
   }
}
