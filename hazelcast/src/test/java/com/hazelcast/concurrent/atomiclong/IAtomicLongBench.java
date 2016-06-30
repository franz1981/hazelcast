package com.hazelcast.concurrent.atomiclong;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.internal.util.ThreadLocalRandom;
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

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;

@State(Scope.Group)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class IAtomicLongBench extends HazelcastTestSupport {

   private static final int CONSUMERS = 4;

   private static final AtomicInteger THREAD_INDEX = new AtomicInteger(0);

   @State(Scope.Thread)
   public static class AtomicLongIndex{

      public final int index = THREAD_INDEX.getAndIncrement();
   }

   private IAtomicLong[] counters;
   private HazelcastInstance instance;

   private static final boolean PAD = true;
   private static final int SPARSE_SHIFT = 5;
   private static final boolean NO_BOUND_CHECKS = true;
   private static final int MAX_PARTITION_COUNT = 271;

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
      //PARTITION_OPERATION_THREAD_COUNT is the hazelcast property to be set to decide the partitions group count
      counters = new IAtomicLong[CONSUMERS];
      final Config config = getConfig();
      config.setProperty(PARTITION_COUNT.getName(),Integer.toString(MAX_PARTITION_COUNT));
      instance = createHazelcastInstanceFactory().newHazelcastInstance(config);
      final int groupSize = MAX_PARTITION_COUNT/CONSUMERS;
      for(int i = 0;i<CONSUMERS;i++) {
         final String name = Integer.toString(i) + "@" + (i*groupSize);
         counters[i] = instance.getAtomicLong(name);
      }
   }

   @Benchmark
   @Group("rw")
   @GroupThreads(CONSUMERS)
   public long inc(AtomicLongIndex atomicLongIndex) {
      final int index = atomicLongIndex.index;
      //final int  index = ThreadLocalRandom.current().nextInt(0,CONSUMERS);
      return counters[index].incrementAndGet();
   }

   /**
   @Benchmark
   @Group("rw")
   @GroupThreads(CONSUMERS)
   public long get(AtomicLongIndex atomicLongIndex) {
      //final int index = atomicLongIndex.index;
      final int  index = ThreadLocalRandom.current().nextInt(0,CONSUMERS);
      return counters[index].get();
   }
    **/

   @TearDown
   public void close(){
      instance.shutdown();
   }


   public static void main(String[] args) throws RunnerException {
      final Options opt = new OptionsBuilder()
         .include(IAtomicLongBench.class.getSimpleName())
         .warmupIterations(5)
         .measurementIterations(5)
         .forks(1)
         .shouldDoGC(true)
         .build();
      new Runner(opt).run();
   }
}
