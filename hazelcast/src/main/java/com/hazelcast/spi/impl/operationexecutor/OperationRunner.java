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

package com.hazelcast.spi.impl.operationexecutor;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import sun.misc.Unsafe;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * The OperationRunner is responsible for the actual running of operations.
 * <p/>
 * So the {@link OperationExecutor} is responsible for 'executing' them (so finding a thread to run on), the actual work is done
 * by the {@link OperationRunner}. This separation of concerns makes the code a lot simpler and makes it possible to swap parts
 * of the system.
 * <p/>
 * Since HZ 3.5 there are multiple OperationRunner instances; each partition will have its own OperationRunner, but also
 * generic threads will have their own OperationRunners. Each OperationRunner exposes the Operation it is currently working
 * on and this makes it possible to hook on all kinds of additional functionality like detecting slow operations, sampling which
 * operations are executed most frequently, check if an operation is still running, etc etc.
 */

abstract class L0PadOperationRunner {

    private static Unsafe findUnsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException se) {
            return AccessController.doPrivileged(new PrivilegedAction<Unsafe>() {
                @Override
                public Unsafe run() {
                    try {
                        Class<Unsafe> type = Unsafe.class;
                        try {
                            Field field = type.getDeclaredField("theUnsafe");
                            field.setAccessible(true);
                            return type.cast(field.get(type));
                        }
                        catch (Exception e) {
                            for (Field field : type.getDeclaredFields()) {
                                if (type.isAssignableFrom(field.getType())) {
                                    field.setAccessible(true);
                                    return type.cast(field.get(type));
                                }
                            }
                        }
                    }
                    catch (Throwable t) {
                        throw rethrow(t);
                    }
                    throw new RuntimeException("Unsafe unavailable");
                }
            });
        }
    }

    protected static final Unsafe UNSAFE;

    static{
        UNSAFE = findUnsafe();
    }

    long l00,l01,l02,l03,l04,l05;
    long l10,l11,l12,l13,l14,l15,l16,l17;
}

abstract class ColdFieldsOperationRunner extends L0PadOperationRunner{
    protected final int partitionId;

    public ColdFieldsOperationRunner(int partitionId){
        this.partitionId = partitionId;
    }

    /**
     * Returns the partitionId this OperationRunner is responsible for. If the partition id is smaller than 0,
     * it is either a generic or ad hoc OperationRunner.
     * <p/>
     * The value will never change for this OperationRunner instance.
     *
     * @return the partition id.
     */
    public final int getPartitionId() {
        return partitionId;
    }
}

abstract class L1PadOperationRunner extends ColdFieldsOperationRunner{
    long l00,l01,l02,l03,l04,l05,l06;
    long l10,l11,l12,l13,l14,l15,l16,l17;

    public L1PadOperationRunner(int partitionId){
        super(partitionId);
    }
}

abstract class CurrentTaskFieldsOperationRunner extends L1PadOperationRunner{
    private static final long CURRENT_TASK_FIELD_OFFSET;

    static{
        try {
            CURRENT_TASK_FIELD_OFFSET = UNSAFE.objectFieldOffset(CurrentTaskFieldsOperationRunner.class.getDeclaredField("currentTask"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

    }

    private Object currentTask;

    public CurrentTaskFieldsOperationRunner(int partitionId){
        super(partitionId);
    }

    /**
     * Returns the current task that is executing. This value could be null if no operation is executing.
     * <p/>
     * Value could be stale as soon as it is returned.
     * <p/>
     * This method is thread-safe as long as the single-writer principle is applied to it: so the thread that executes a task will set/unset the current task,
     * any other thread in the system is allowed to read it.
     *
     * @return the current running task.
     */
    public final Object currentTask() {
        return UNSAFE.getObjectVolatile(this,CURRENT_TASK_FIELD_OFFSET);
    }

    protected final Object loadPlainCurrentTask(){
        return currentTask;
    }

    protected final void storeOrderedCurrentTask(final Object task){
        UNSAFE.putOrderedObject(this,CURRENT_TASK_FIELD_OFFSET,task);
    }
}


abstract class L2PadOperationRunner extends CurrentTaskFieldsOperationRunner{
    long l00,l01,l02,l03,l04,l05,l06;
    long l10,l11,l12,l13,l14,l15,l16,l17;

    public L2PadOperationRunner(int partitionId){
        super(partitionId);
    }
}


abstract class CurrentThreadOperationRunner extends L2PadOperationRunner{
    private Thread currentThread;

    private static final long CURRENT_THREAD_FIELD_OFFSET;

    static{
        try {
            CURRENT_THREAD_FIELD_OFFSET = UNSAFE.objectFieldOffset(CurrentThreadOperationRunner.class.getDeclaredField("currentThread"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

    }

    public CurrentThreadOperationRunner(int partitionId){
        super(partitionId);
    }

    /**
     * Sets the thread that is running this OperationRunner instance.
     * <p/>
     * This method is thread-safe as long as the single-writer principle is applied to it and it should only be
     * called from the {@link OperationExecutor} since this component is responsible for
     * executing operations on an OperationRunner.
     *
     * @param currentThread the current Thread. Can be called with 'null', clearing the currentThread field.
     */
    public final void setCurrentThread(Thread currentThread) {
        storeOrderedCurrentThread(currentThread);
    }

    /**
     * Get the thread that is currently running this OperationRunner instance.
     * <p/>
     * This value only has meaning when an Operation is running. It depends on the implementation if the field is unset
     * after an operation is executed or not. So it could be that a value is returned while no operation is running.
     * <p/>
     * For example, the {@link OperationExecutorImpl} will never unset
     * this field since each OperationRunner is bound to a single OperationThread; so this field is initialized when the
     * OperationRunner is created.
     * <p/>
     * The returned value could be null. When it is null, currently no thread is running this OperationRunner.
     * <p/>
     * Recommended idiom for slow operation detection:
     * 1: First read the operation and store the reference.
     * 2: Then read the current thread and store the reference
     * 3: Later read the operation again. If the operation-instance is the same, it means that you have captured the right thread.
     * <p/>
     * Then you use this Thread to create a stack trace. It can happen that the stracktrace doesn't reflect the call-state the
     * thread had when the slow operation was detected. This could be solved by rechecking the currentTask after you have detected
     * the slow operation. BUt don't create a stacktrace before you do the first recheck of the operation because otherwise it
     * will cause a lot of overhead.
     *
     * @return the Thread that currently is running this OperationRunner instance.
     */
    public final Thread currentThread() {
        return (Thread)UNSAFE.getObjectVolatile(this,CURRENT_THREAD_FIELD_OFFSET);
    }

    protected final Object loadPlainCurrentThread(){
        return currentThread;
    }

    protected final void storeOrderedCurrentThread(final Thread thread){
        UNSAFE.putOrderedObject(this,CURRENT_THREAD_FIELD_OFFSET,thread);
    }

}

public abstract class OperationRunner extends CurrentThreadOperationRunner{

    long l00,l01,l02,l03,l04,l05,l06;
    long l10,l11,l12,l13,l14,l15,l16,l17;

    public OperationRunner(int partitionId) {
        super(partitionId);
    }

    public abstract void run(Packet packet) throws Exception;

    public abstract void run(Runnable task);

    public abstract void run(Operation task);


}
