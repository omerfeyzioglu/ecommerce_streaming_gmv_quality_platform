package com.example.flinkkafkasandbox;

import java.io.Serializable;

public final class GmvAccumulator implements Serializable {
    private double totalGmv;
    private long orderCount;

    public void add(double amount) {
        totalGmv += amount;
        orderCount += 1;
    }

    public void merge(GmvAccumulator other) {
        totalGmv += other.totalGmv;
        orderCount += other.orderCount;
    }

    public double totalGmv() {
        return totalGmv;
    }

    public long orderCount() {
        return orderCount;
    }
}
