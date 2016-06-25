package com.hazelcast.spi.impl.operationexecutor.impl;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.hazelcast.nio.Bits;
import sun.misc.Unsafe;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Sparse and Padded AtomicIntegerArray implementation.
 *
 * @see java.util.concurrent.atomic.AtomicIntegerArray
 */
final class SparsePaddedAtomicIntegerArray {

   private static final Unsafe unsafe;
   private static final int ARRAY_PAD;
   private static final int ARRAY_BASE;
   private static final int ARRAY_ELEMENT_SHIFT;
   private static final int SPARSE_SHIFT;
   private static final boolean BOUNDS_CHECK;

   static {
      unsafe = findUnsafe();
      final int arrayBaseOffset = unsafe.arrayBaseOffset(int[].class);
      final int arrayIndexScale = unsafe.arrayIndexScale(int[].class);
      final int arrayElementShift = 31 - Integer.numberOfLeadingZeros(arrayIndexScale);
      //NOTE: without a proper configuration the SparsePaddedAtomicIntegerArray behaves like AtomicIntegerArray
      final boolean pad = Boolean.getBoolean("com.hazelcast.array.pad");
      if (pad) {
         ARRAY_PAD = (Bits.CACHE_LINE_LENGTH * 2) >> arrayElementShift;
         final int paddingOffset = ARRAY_PAD << arrayElementShift;
         ARRAY_BASE = arrayBaseOffset + paddingOffset;
      }
      else {
         ARRAY_PAD = 0;
         ARRAY_BASE = arrayBaseOffset;
      }
      final int sparseShift = Integer.getInteger("com.hazelcast.array.sparse.shift", 0);
      SPARSE_SHIFT = sparseShift;
      ARRAY_ELEMENT_SHIFT = arrayElementShift + sparseShift;
      BOUNDS_CHECK = !Boolean.getBoolean("com.hazelcast.disable.array.bounds.checks");
   }

   private final int[] array;
   private final int length;

   public SparsePaddedAtomicIntegerArray(int capacity) {
      this.array = new int[requiredCapacity(capacity)];
      this.length = capacity;
   }

   public SparsePaddedAtomicIntegerArray(int[] var) {
      this(var.length);
      for (int i = 0; i < var.length; i++) {
         final int value = var[i];
         final long offset = offset(i);
         unsafe.putInt(this.array, offset, value);
      }
   }

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

   private static int requiredCapacity(final int capacity) {
      return (capacity << SPARSE_SHIFT) + ARRAY_PAD * 2;
   }

   private static long offset(final int index) {
      return ARRAY_BASE + (index << ARRAY_ELEMENT_SHIFT);
   }

   private static void checkIndex(final int index, final int capacity) {
      if (!(index >= 0 && index < capacity)) {
         throw new IndexOutOfBoundsException("index " + index);
      }
   }

   public final int length() {
      return length;
   }

   public final int get(int index) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return this.unsafe.getInt(this.array, offset(index));
   }

   public final void set(int index, int value) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      unsafe.putIntVolatile(this.array, this.offset(index), value);
   }

   public final void lazySet(int index, int value) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      unsafe.putOrderedInt(this.array, this.offset(index), value);
   }

   public final int getAndSet(int index, int value) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndSetInt(this.array, this.offset(index), value);
   }

   public final boolean compareAndSet(int index, int expect, int update) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.compareAndSwapInt(this.array, offset(index), expect, update);
   }

   public final boolean weakCompareAndSet(int index, int expect, int update) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      //There is no distinction between this and compareAndSet in the original Atomic* class
      return unsafe.compareAndSwapInt(this.array, offset(index), expect, update);
   }

   public final int getAndIncrement(int index) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndAddInt(this.array, offset(index), 1);
   }

   public final int getAndDecrement(int index) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndAddInt(this.array, offset(index), -1);
   }

   public final int getAndAdd(int index, int value) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndAddInt(this.array, offset(index), value);
   }

   public final int incrementAndGet(int index) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndAddInt(this.array, offset(index), 1) + 1;
   }

   public final int decrementAndGet(int index) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndAddInt(this.array, offset(index), -1) - 1;
   }

   public final int addAndGet(int index, int value) {
      if (BOUNDS_CHECK) {
         checkIndex(index, length);
      }
      return unsafe.getAndAddInt(this.array, offset(index), value) + value;
   }

   public String toString() {
      if (length == 0) {
         return "[]";
      }
      final StringBuilder builder = new StringBuilder();
      builder.append('[');
      builder.append(this.get(0));
      for (int i = 1; i < length; i++) {
         builder.append(',').append(' ').append(this.get(i));
      }
      builder.append(']');
      return builder.toString();
   }
}
