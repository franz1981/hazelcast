package com.hazelcast.spi.impl.operationexecutor.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.RuntimeInterruptedException;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_OPERATION_THREAD_COUNT;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class SparsePaddedAtomicIntegerArrayBench{
   private static final int CONSUMERS = 4;

   private static final AtomicInteger THREAD_INDEX = new AtomicInteger(0);

   private SparsePaddedAtomicIntegerArray counters;

   private static final int GROUP_COUNT = 1 << (32 - Integer.numberOfLeadingZeros(Runtime.getRuntime().availableProcessors() - 1));
   private static final boolean PAD = true;
   private static final int SPARSE_SHIFT = 5;
   private static final boolean NO_BOUND_CHECKS = true;

   @State(Scope.Thread)
   public static class ThreadIndex {

      public final int index = THREAD_INDEX.getAndIncrement()&(GROUP_COUNT-1);

   }

   static{
      //com.hazelcast.array.pad":boolean -> if true add pad at begin/end of partitions spin-lock array
      //com.hazelcast.array.sparse.shift":integer -> bit shift that separate elements of the spin-lock array to reduce false sharing
      //com.hazelcast.disable.array.bounds.checks:boolean -> true if want to disable bound check on group lock array
      System.setProperty("com.hazelcast.array.pad",Boolean.toString(PAD));
      System.setProperty("com.hazelcast.array.sparse.shift",Integer.toString(SPARSE_SHIFT));
      System.setProperty("com.hazelcast.disable.array.bounds.checks",Boolean.toString(NO_BOUND_CHECKS));
   }

   @Setup
   public void init(){
      counters = new SparsePaddedAtomicIntegerArray(GROUP_COUNT);
   }

   @Benchmark
   @Group("rw")
   @GroupThreads(CONSUMERS)
   public int incrementAndGet(ThreadIndex threadIndex) {
      final int index = threadIndex.index;
      return counters.incrementAndGet(index);
   }

   public static void main(String[] args) throws RunnerException {
      final Options opt = new OptionsBuilder()
         .include(SparsePaddedAtomicIntegerArrayBench.class.getSimpleName())
         .warmupIterations(5)
         .measurementIterations(5)
         .forks(1)
         .shouldDoGC(true)
         .build();
      new Runner(opt).run();
   }
}
