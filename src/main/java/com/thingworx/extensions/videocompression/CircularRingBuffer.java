package com.thingworx.extensions.videocompression;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;

public class CircularRingBuffer<T> {

    private T[] buffer;

    private int tail;

    private int head;

    @SuppressWarnings("unchecked")
    public CircularRingBuffer(int n) {
        buffer = (T[]) new Object[n];
        tail = 0;
        head = 0;
    }

    public void add(T toAdd) {
        if (head != (tail - 1)) {
            buffer[head++] = toAdd;
        } else {
            throw new BufferOverflowException();
        }
        head = head % buffer.length;
    }

    public T get() {
        T t;
        int adjTail = tail > head ? tail - buffer.length : tail;
        if (adjTail < head) {
            t = buffer[tail++];
            tail = tail % buffer.length;
        } else {
            throw new BufferUnderflowException();
        }
        return t;
    }

    public List<T> getAll() {
        List<T> list = new ArrayList<>();
        while (true) {
            try {
                list.add(this.get());
            } catch (BufferUnderflowException e) {
                break;
            }
        }
        return list;
    }

    public String toString() {
        return "CircularBuffer(size=" + buffer.length + ", head=" + head + ", tail=" + tail + ")";
    }

}