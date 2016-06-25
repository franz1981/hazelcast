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

import java.util.concurrent.LinkedBlockingQueue;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
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

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.spi.properties.GroupProperty.GENERIC_OPERATION_THREAD_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
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
 * <p/>
 * There are 2 category of operations:
 * <ol>
 * <li>partition specific operations: performed by the caller thread, these operations are guarded by a proper wait-free/spin-lock to protect the partitions consistency
 * </li>
 * <li>
 * generic operations: these operations are responsible for executing task specific to a partition and can eventually be executed by a fixed thread pool. E.g. a heart beat.
 * </li>
 *
 * </ol>
 */
@SuppressWarnings("checkstyle:methodcount")
public final class OperationExecutorImpl implements OperationExecutor, MetricsProvider {

   private static final int TERMINATION_TIMEOUT_SECONDS = 3;

   private final ILogger logger;

   // all operations for specific partitions will be executed by there partitionOperationRunners, e.g. map.put(key, value)
   private final OperationRunner[] partitionOperationRunners;
   private final ReentrantGroupLock groupLock;
   private final int groupsMask;

   private final OperationQueue genericQueue = new DefaultOperationQueue(new LinkedBlockingQueue<Object>(), new LinkedBlockingQueue<Object>());

   // all operations that are not specific for a partition will be executed here, e.g. heartbeat or map.size()
   private final GenericOperationThread[] genericThreads;
   private final OperationRunner[] genericOperationRunners;

   private final Address thisAddress;
   private final OperationRunner adHocOperationRunner;
   private final int priorityThreadCount;

   public OperationExecutorImpl(HazelcastProperties properties,
                                LoggingService loggerService,
                                Address thisAddress,
                                OperationRunnerFactory operationRunnerFactory,
                                HazelcastThreadGroup threadGroup,
                                NodeExtension nodeExtension) {
      this.thisAddress = thisAddress;
      this.logger = loggerService.getLogger(OperationExecutorImpl.class);
      this.adHocOperationRunner = operationRunnerFactory.createAdHocRunner();
      this.partitionOperationRunners = initPartitionOperationRunners(properties, operationRunnerFactory);
      this.groupLock = initPartitionGroupLock(properties);
      this.groupsMask = this.groupLock.groups() - 1;
      this.priorityThreadCount = properties.getInteger(PRIORITY_GENERIC_OPERATION_THREAD_COUNT);
      this.genericOperationRunners = initGenericOperationRunners(properties, operationRunnerFactory);
      this.genericThreads = initGenericThreads(threadGroup, nodeExtension);
   }

   private static final int groupIndex(int partitionId, int groupsMask) {
      return partitionId & groupsMask;
   }

   //TODO Add tests and find a proper util class to put it
   private static int nextPowOfTwo(final int value) {
      if (value > (1 << 30)) {
         throw new IllegalArgumentException("next pow of 2 cannot exceeds 2^31!");
      }
      final int nextPow2 = 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
      return nextPow2;
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
      //use old
      int groupsCount = properties.getInteger(PARTITION_OPERATION_THREAD_COUNT);
      if (groupsCount <= 0) {
         groupsCount = Runtime.getRuntime().availableProcessors();
      }
      //approx to next pow2 value to enable fast mod operation
      groupsCount = nextPowOfTwo(groupsCount);
      return new ReentrantGroupLock(groupsCount);
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
      //TODO:
      //Find if there are specific PartitionOperationThread's collected metrics to be replaced
      //metricsRegistry.collectMetrics((Object[]) partitionThreads);
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
      //TODO:
      //Add the threads count that are currently retrying to lock a partition's group?
      return genericQueue.normalSize();
   }

   @Override
   @Probe(name = "priorityQueueSize", level = MANDATORY)
   public int getPriorityQueueSize() {
      //TODO:
      //Add the threads count that are currently retrying to lock a partition's group?
      return genericQueue.prioritySize();
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
   public int getPartitionThreadCount() { return this.groupLock.groups(); }

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
      final int partitionId = op.getPartitionId();
      final boolean priority = op.isUrgent();
      if (partitionId < 0) {
         genericQueue.add(op, priority);
      }
      else {
         final int groupIndex = groupIndex(partitionId,groupsMask);
         final Thread currentThread = Thread.currentThread();
         while(!groupLock.tryLock(groupIndex)){
            if(currentThread.isInterrupted()){
               throw new IllegalStateException("can't execute the task if interrupted!");
            }
         }
         OperationRunner operationRunner = null;
         try{
            operationRunner = partitionOperationRunners[partitionId];
            operationRunner.setCurrentThread(currentThread);
            operationRunner.run(op);
         }finally{
            if(operationRunner!=null) {
               operationRunner.setCurrentThread(null);
            }
            this.groupLock.unlock(groupIndex);
         }
      }
   }

   @Override
   public void execute(PartitionSpecificRunnable task) {
      checkNotNull(task, "task can't be null");
      final int partitionId = task.getPartitionId();
      final boolean priority = task instanceof UrgentSystemOperation;
      if (partitionId < 0) {
         genericQueue.add(task, priority);
      }
      else {
         final int groupIndex = groupIndex(partitionId,groupsMask);
         final Thread currentThread = Thread.currentThread();
         while(!groupLock.tryLock(groupIndex)){
            if(currentThread.isInterrupted()){
               throw new IllegalStateException("can't execute the task if interrupted!");
            }
         }
         OperationRunner operationRunner = null;
         try{
            operationRunner = partitionOperationRunners[partitionId];
            operationRunner.setCurrentThread(currentThread);
            operationRunner.run(task);
         }finally{
            if(operationRunner!=null) {
               operationRunner.setCurrentThread(null);
            }
            this.groupLock.unlock(groupIndex);
         }
      }
   }

   @Override
   public void handle(Packet packet) {
      checkNotNull(packet, "packet can't be null");
      final int partitionId = packet.getPartitionId();
      final boolean priority = packet.isUrgent();
      if (partitionId < 0) {
         genericQueue.add(packet, priority);
      }
      else {
         final int groupIndex = groupIndex(partitionId,groupsMask);
         final Thread currentThread = Thread.currentThread();
         while(!groupLock.tryLock(groupIndex)){
            if(currentThread.isInterrupted()){
               throw new IllegalStateException("can't execute the task if interrupted!");
            }
         }
         OperationRunner operationRunner = null;
         try {
            operationRunner = partitionOperationRunners[partitionId];
            operationRunner.setCurrentThread(currentThread);
            operationRunner.run(packet);
         }catch (Throwable t){
            inspectOutOfMemoryError(t);
            logger.severe("Failed to process packet: " + packet + " on " + currentThread, t);
         }finally{
            if(operationRunner!=null) {
               operationRunner.setCurrentThread(null);
            }
            this.groupLock.unlock(groupIndex);
         }
      }
   }

   @Override
   @Deprecated
   public void executeOnPartitionThreads(Runnable task) {
      checkNotNull(task, "task can't be null");
      //TODO mimic old behaviour -> execute for each group
      final int groups = this.groupLock.groups();
      for(int groupIndex = 0;groupIndex<groups;groupIndex++){
         executeOnGroupId(task,groupIndex);
      }
   }

   private void executeOnGroupId(final Runnable task, final int groupIndex){
      final Thread currentThread = Thread.currentThread();
      while(!groupLock.tryLock(groupIndex)){
         if(currentThread.isInterrupted()){
            throw new IllegalStateException("can't execute the task if interrupted!");
         }
      }
      try{
         task.run();
      }finally{
         this.groupLock.unlock(groupIndex);
      }
   }

   @Override
   @Deprecated
   public void interruptPartitionThreads() {
      //TODO Find an equivalent behaviour that "close" the GroupLock and refuse future attempts of executions!
   }

   @Override
   public void run(Operation operation) {
      checkNotNull(operation, "operation can't be null");
      if (!isRunAllowed(operation)) {
         throw new IllegalThreadStateException("Operation '" + operation + "' cannot be run in current thread: " + Thread.currentThread());
      }
      final int partitionId = operation.getPartitionId();
      if (partitionId < 0) {
         final OperationRunner operationRunner;
         final Thread currentThread = Thread.currentThread();
         if (!(currentThread instanceof OperationThread)) {
            // if thread is not an operation thread, we return the adHocOperationRunner
            operationRunner = adHocOperationRunner;
         }else {
            // It is a generic operation and we are running on an operation-thread. So we can just return the operation-runner
            // for that thread. There won't be any partition-conflict since generic operations are allowed to be executed by
            // a partition-specific operation-runner.
            final OperationThread operationThread = (OperationThread) currentThread;
            operationRunner = operationThread.currentRunner;
         }
         //TODO verify corner case
         //if the operationRunner is related to a specific partitionId it needs to be guarded by the group lock?
         final int runnerPartitionId = operationRunner.getPartitionId();
         if(runnerPartitionId<0){
            operationRunner.run(operation);
         }else{
            executeOnPartition(operation,runnerPartitionId,operationRunner);
         }
      }else {
         final OperationRunner operationRunner = partitionOperationRunners[partitionId];
         executeOnPartition(operation,partitionId,operationRunner);
      }
   }

   private void executeOnPartition(final Operation operation,final int partitionId,final OperationRunner operationRunner){
      final int groupIndex = groupIndex(partitionId, groupsMask);
      final Thread currentThread = Thread.currentThread();
      while (!groupLock.tryLock(groupIndex)) {
         if (currentThread.isInterrupted()) {
            throw new IllegalStateException("can't execute the task if interrupted!");
         }
      }
      try {
         operationRunner.setCurrentThread(currentThread);
         operationRunner.run(operation);
      }
      finally {
         if (operationRunner != null) {
            operationRunner.setCurrentThread(null);
         }
         this.groupLock.unlock(groupIndex);
      }
   }

   @Override
   public void runOrExecute(Operation op) {
      if (isRunAllowed(op)) {
         run(op);
      }
      else {
         execute(op);
      }
   }

   @Override
   public boolean isRunAllowed(Operation op) {
      checkNotNull(op, "op can't be null");
      final Thread currentThread = Thread.currentThread();
      // IO threads are not allowed to run any operation
      if (currentThread instanceof OperationHostileThread) {
         return false;
      }
      return true;
   }

   @Override
   public boolean isInvocationAllowed(Operation op, boolean isAsync) {
      checkNotNull(op, "op can't be null");
      final Thread currentThread = Thread.currentThread();
      // IO threads are not allowed to run any operation
      if (currentThread instanceof OperationHostileThread) {
         return false;
      }
      return true;
   }

   // public for testing purposes
   @Deprecated
   public int toPartitionThreadIndex(int partitionId) {
      return groupIndex(partitionId,groupsMask);
   }

   @Override
   public void start() {
      logger.info("Starting " + genericThreads.length + " generic threads (" + priorityThreadCount + " dedicated for priority tasks)");
      startAll(genericThreads);
   }

   @Override
   public void shutdown() {
      shutdownAll(genericThreads);
      //TODO wait until all the group lock are released or check if they are released and throw an exception if not?

      awaitTermination(genericThreads);
   }

   @Override
   public String toString() {
      return "OperationExecutorImpl{node=" + thisAddress + '}';
   }
}
