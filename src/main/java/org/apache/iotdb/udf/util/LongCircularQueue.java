/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.udf.util;

/** Circular queue for long */
public class LongCircularQueue {

  private static int INITCAP = 64;

  private int head, tail, size, minLen;
  private long[] data;

  public LongCircularQueue(int capacity) {
    head = tail = size = 0;
    data = new long[capacity];
    minLen = Math.max(INITCAP, capacity);
  }

  public LongCircularQueue() {
    this(INITCAP);
  }

  /**
   * push value into the back of queue
   *
   * @param value value to push
   */
  public void push(long value) {
    if (isFull()) {
      resize(data.length * 2);
    }
    data[tail] = value;
    tail = (tail + 1) % data.length;
    size++;
  }

  /**
   * pop value from the front of queue
   *
   * @return value in the front
   */
  public long pop() {
    if (isEmpty()) {
      throw new IllegalArgumentException("Error: Queue is Empty!");
    }
    long ret = data[head];
    head = (head + 1) % data.length;
    size--;
    if (size < data.length / 4 && data.length / 2 >= minLen) {
      resize(data.length / 2);
    }
    return ret;
  }

  /**
   * judge if circular queue is empty
   *
   * @return if empty, return true; else return false
   */
  public long getHead() {
    if (isEmpty()) {
      throw new IllegalArgumentException("Error: Queue is Empty!");
    }
    long ret = data[head];
    return ret;
  }

  /**
   * judge if circular queue is empty
   *
   * @return if empty, return true; else return false
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * judge if circular queue is full
   *
   * @return if full, return true; else return false
   */
  private boolean isFull() {
    return size == data.length;
  }

  /**
   * change size of queue
   *
   * @param newLength new length of queue
   */
  private void resize(int newLength) {
    long[] newData = new long[newLength];
    for (int i = 0; i < size; i++) {
      newData[i] = data[(head + i) % data.length];
    }
    data = newData;
    head = 0;
    tail = size;
  }

  /**
   * get value of given index
   *
   * @param index index
   * @return value to queue
   */
  public long get(int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException();
    }
    return data[(head + index) % data.length];
  }

  /**
   * get number of values in queue
   *
   * @return number of values
   */
  public int getSize() {
    return size;
  }
}
