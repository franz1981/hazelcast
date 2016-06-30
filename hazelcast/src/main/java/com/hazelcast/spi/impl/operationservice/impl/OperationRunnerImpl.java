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

package com.hazelcast.spi.impl.operationservice.impl;

import java.util.logging.Level;

import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.ExceptionUtil;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.nio.IOUtil.extractOperationCallId;
import static com.hazelcast.spi.Operation.CALL_ID_LOCAL_SKIPPED;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setConnection;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static com.hazelcast.spi.properties.GroupProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

abstract class ColdFieldsOperationRunnerImpl extends OperationRunner implements MetricsProvider {
   protected final ILogger logger;
   protected final OperationServiceImpl operationService;
   protected final Node node;
   protected final NodeEngineImpl nodeEngine;

   @Probe(level = DEBUG)
   protected final Counter count;
   protected final Address thisAddress;
   protected final boolean staleReadOnMigrationEnabled;

   protected final OperationResponseHandler remoteResponseHandler;

   // When partitionId >= 0, it is a partition specific
   // when partitionId = -1, it is generic
   // when partitionId = -2, it is ad hoc
   // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
   // and therefor the {@link OperationRunner#currentTask()} always returns null
   ColdFieldsOperationRunnerImpl(OperationServiceImpl operationService, int partitionId) {
      super(partitionId);
      this.operationService = operationService;
      this.logger = operationService.node.getLogger(OperationRunnerImpl.class);
      this.node = operationService.node;
      this.thisAddress = node.getThisAddress();
      this.nodeEngine = operationService.nodeEngine;
      this.remoteResponseHandler = new RemoteInvocationResponseHandler(operationService);
      this.staleReadOnMigrationEnabled = !node.getProperties().getBoolean(DISABLE_STALE_READ_ON_PARTITION_MIGRATION);

      if (partitionId >= 0) {
         this.count = newSwCounter();
      }
      else {
         this.count = null;
      }
   }

}

abstract class L1PadOperationRunnerImpl extends ColdFieldsOperationRunnerImpl {

   long l00, l01, l02, l03;
   long l10, l11, l12, l13, l14, l15, l16, l17;

   L1PadOperationRunnerImpl(OperationServiceImpl operationService, int partitionId) {
      super(operationService,partitionId);
   }
}

abstract class InternalPartitionOperationRunnerImpl extends L1PadOperationRunnerImpl{
   // This field doesn't need additional synchronization, since a partition-specific OperationRunner
   // will never be called concurrently.
   protected InternalPartition internalPartition;

   InternalPartitionOperationRunnerImpl(OperationServiceImpl operationService, int partitionId) {
      super(operationService,partitionId);
   }

}

/**
 * Responsible for processing an Operation.
 */
final class OperationRunnerImpl extends InternalPartitionOperationRunnerImpl {

   static final int AD_HOC_PARTITION_ID = -2;
   long l00,l01,l02,l03,l04,l05,l06;
   long l10,l11,l12,l13,l14,l15,l16,l17;

   OperationRunnerImpl(OperationServiceImpl operationService, int partitionId) {
      super(operationService,partitionId);
   }




   @Override
   public void provideMetrics(MetricsRegistry metricsRegistry) {
      if (partitionId >= 0) {
         metricsRegistry.scanAndRegister(this, "operation.partition[" + partitionId + "]");
      }
   }

   @Override
   public void run(Runnable task) {
      final Object localCurrentTask = loadPlainCurrentTask();
      final int partitionId = getPartitionId();
      final boolean publishCurrentTask = publishTask(localCurrentTask, partitionId);
      if (publishCurrentTask) {
         storeOrderedCurrentTask(task);
      }
      try {
         task.run();
      }
      finally {
         if (publishCurrentTask) {
            storeOrderedCurrentTask(null);
         }
      }
   }

   private static boolean publishTask(final Object task, final int partitionId) {
      final boolean isClientRunnable = task instanceof MessageTask;
      return (partitionId != AD_HOC_PARTITION_ID && (task == null || isClientRunnable));
   }

   @Override
   public void run(final Operation operation) {
      if (count != null) {
         count.inc();
      }
      operationService.completedOperationsCount.incrementAndGet();
      final Object localCurrentTask = loadPlainCurrentTask();
      final int partitionId = getPartitionId();
      final boolean publishCurrentTask = publishTask(localCurrentTask, partitionId);
      if (publishCurrentTask) {
         storeOrderedCurrentTask(operation);
      }
      try {
         checkNodeState(operation);

         if (timeout(operation)) {
            return;
         }

         ensureNoPartitionProblems(operation);

         ensureQuorumPresent(operation);

         operation.beforeRun();

         if (waitingNeeded(operation)) {
            return;
         }

         operation.run();
         handleResponse(operation);
         afterRun(operation);
      }
      catch (Throwable e) {
         handleOperationError(operation, e);
      }
      finally {
         if (publishCurrentTask) {
            storeOrderedCurrentTask(null);
         }
      }
   }

   private void checkNodeState(Operation op) {
      NodeState state = node.getState();
      if (state == NodeState.ACTIVE) {
         return;
      }

      if (state == NodeState.SHUT_DOWN) {
         throw new HazelcastInstanceNotActiveException("This node is shut down! Operation: " + op);
      }

      if (op instanceof AllowedDuringPassiveState) {
         return;
      }

      // Cluster is in passive state. There is no need to retry.
      if (nodeEngine.getClusterService().getClusterState() == ClusterState.PASSIVE) {
         throw new IllegalStateException("Cluster is in " + ClusterState.PASSIVE + " state! Operation: " + op);
      }

      // Operation has no partition id. So it is sent to this node in purpose.
      // Operation will fail since node is shutting down or cluster is passive.
      if (op.getPartitionId() < 0) {
         throw new HazelcastInstanceNotActiveException("This node is currently passive! Operation: " + op);
      }

      // Custer is not passive but this node is shutting down.
      // Since operation has a partition id, it must be retried on another node.
      throw new RetryableHazelcastException("This node is currently shutting down! Operation: " + op);
   }

   private void ensureQuorumPresent(Operation op) {
      QuorumServiceImpl quorumService = operationService.nodeEngine.getQuorumService();
      quorumService.ensureQuorumPresent(op);
   }

   private boolean waitingNeeded(Operation op) {
      if (!(op instanceof BlockingOperation)) {
         return false;
      }

      BlockingOperation blockingOperation = (BlockingOperation) op;
      if (blockingOperation.shouldWait()) {
         nodeEngine.getWaitNotifyService().await(blockingOperation);
         return true;
      }
      return false;
   }

   private boolean timeout(Operation op) {
      if (!operationService.isCallTimedOut(op)) {
         return false;
      }

      op.sendResponse(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
      return true;
   }

   private void handleResponse(Operation op) throws Exception {
      boolean returnsResponse = op.returnsResponse();
      int backupAcks = sendBackup(op);

      if (!returnsResponse) {
         return;
      }

      sendResponse(op, backupAcks);
   }

   private int sendBackup(Operation op) throws Exception {
      if (!(op instanceof BackupAwareOperation)) {
         return 0;
      }

      int backupAcks = 0;
      BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
      if (backupAwareOp.shouldBackup()) {
         backupAcks = operationService.operationBackupHandler.backup(backupAwareOp);
      }
      return backupAcks;
   }

   private void sendResponse(Operation op, int backupAcks) {
      try {
         Object response = op.getResponse();
         if (backupAcks > 0) {
            response = new NormalResponse(response, op.getCallId(), backupAcks, op.isUrgent());
         }
         op.sendResponse(response);
      }
      catch (ResponseAlreadySentException e) {
         logOperationError(op, e);
      }
   }

   private void afterRun(Operation op) {
      try {
         op.afterRun();
         if (op instanceof Notifier) {
            final Notifier notifier = (Notifier) op;
            if (notifier.shouldNotify()) {
               operationService.nodeEngine.getWaitNotifyService().notify(notifier);
            }
         }
      }
      catch (Throwable e) {
         // passed the response phase
         // `afterRun` and `notifier` errors cannot be sent to the caller anymore
         // just log the error
         logOperationError(op, e);
      }
   }

   private void ensureNoPartitionProblems(Operation op) {
      int partitionId = op.getPartitionId();

      if (partitionId < 0) {
         return;
      }

      if (partitionId != getPartitionId()) {
         throw new IllegalStateException("wrong partition, expected: " + getPartitionId() + " but found:" + partitionId);
      }

      if (internalPartition == null) {
         internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
      }

      if (retryDuringMigration(op) && internalPartition.isMigrating()) {
         throw new PartitionMigratingException(thisAddress, partitionId, op.getClass().getName(), op.getServiceName());
      }

      Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
      if (op.validatesTarget() && !thisAddress.equals(owner)) {
         throw new WrongTargetException(thisAddress, owner, partitionId, op.getReplicaIndex(), op.getClass().getName(), op.getServiceName());
      }
   }

   private boolean retryDuringMigration(Operation op) {
      return !((op instanceof ReadonlyOperation && staleReadOnMigrationEnabled) || isMigrationOperation(op));
   }

   private void handleOperationError(Operation operation, Throwable e) {
      if (e instanceof OutOfMemoryError) {
         OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
      }
      try {
         operation.onExecutionFailure(e);
      }
      catch (Throwable t) {
         logger.warning("While calling 'operation.onFailure(e)'... op: " + operation + ", error: " + e, t);
      }

      operation.logError(e);

      if (!operation.returnsResponse()) {
         return;
      }

      OperationResponseHandler responseHandler = operation.getOperationResponseHandler();
      try {
         if (node.getState() != NodeState.SHUT_DOWN) {
            responseHandler.sendResponse(operation, e);
         }
         else if (responseHandler.isLocal()) {
            responseHandler.sendResponse(operation, new HazelcastInstanceNotActiveException());
         }
      }
      catch (Throwable t) {
         logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
      }
   }

   private void logOperationError(Operation op, Throwable e) {
      if (e instanceof OutOfMemoryError) {
         OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
      }
      op.logError(e);
   }

   @Override
   public void run(Packet packet) throws Exception {
      final Object localCurrentTask = loadPlainCurrentTask();
      final int partitionId = getPartitionId();
      final boolean publishCurrentTask = publishTask(localCurrentTask, partitionId);
      if (publishCurrentTask) {
         storeOrderedCurrentTask(packet);
      }

      Connection connection = packet.getConn();
      Address caller = connection.getEndPoint();
      boolean alreadyPublished = false;
      try {
         Object object = nodeEngine.toObject(packet);
         Operation op = (Operation) object;
         op.setNodeEngine(nodeEngine);
         setCallerAddress(op, caller);
         setConnection(op, connection);
         setCallerUuidIfNotSet(caller, op);
         setOperationResponseHandler(op);

         if (!ensureValidMember(op)) {
            return;
         }
         if (publishCurrentTask) {
            storeOrderedCurrentTask(null);
            alreadyPublished = true;
         }
         run(op);
      }
      catch (Throwable throwable) {
         // If exception happens we need to extract the callId from the bytes directly!
         long callId = extractOperationCallId(packet, node.getSerializationService());
         operationService.send(new ErrorResponse(throwable, callId, packet.isUrgent()), caller);
         logOperationDeserializationException(throwable, callId);
         throw ExceptionUtil.rethrow(throwable);
      }
      finally {
         if (publishCurrentTask && !alreadyPublished) {
            storeOrderedCurrentTask(null);
         }
      }
   }

   private void setOperationResponseHandler(Operation op) {
      OperationResponseHandler handler = remoteResponseHandler;
      if (op.getCallId() == 0 || op.getCallId() == CALL_ID_LOCAL_SKIPPED) {
         if (op.returnsResponse()) {
            throw new HazelcastException("Op: " + op + " can not return response without call-id!");
         }
         handler = createEmptyResponseHandler();
      }
      op.setOperationResponseHandler(handler);
   }

   private boolean ensureValidMember(Operation op) {
      if (node.clusterService.getMember(op.getCallerAddress()) != null || isJoinOperation(op) || isWanReplicationOperation(op)) {
         return true;
      }

      Exception error = new CallerNotMemberException(thisAddress, op.getCallerAddress(), op.getPartitionId(), op.getClass().getName(), op.getServiceName());
      handleOperationError(op, error);
      return false;
   }

   private void setCallerUuidIfNotSet(Address caller, Operation op) {
      if (op.getCallerUuid() != null) {
         return;

      }
      MemberImpl callerMember = node.clusterService.getMember(caller);
      if (callerMember != null) {
         op.setCallerUuid(callerMember.getUuid());
      }
   }

   private void logOperationDeserializationException(Throwable t, long callId) {
      boolean returnsResponse = callId != 0;

      if (t instanceof RetryableException) {
         final Level level = returnsResponse ? FINEST : WARNING;
         if (logger.isLoggable(level)) {
            logger.log(level, t.getClass().getName() + ": " + t.getMessage());
         }
      }
      else if (t instanceof OutOfMemoryError) {
         try {
            logger.log(SEVERE, t.getMessage(), t);
         }
         catch (Throwable ignored) {
            logger.log(SEVERE, ignored.getMessage(), t);
         }
      }
      else {
         final Level level = nodeEngine.isRunning() ? SEVERE : FINEST;
         if (logger.isLoggable(level)) {
            logger.log(level, t.getMessage(), t);
         }
      }
   }
}
