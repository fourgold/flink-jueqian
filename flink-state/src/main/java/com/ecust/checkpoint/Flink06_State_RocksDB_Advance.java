package com.ecust.checkpoint;

/**
 * @author JueQian
 * @create 01-19 14:54
 * RocksDB是一个本地库，
 * 它直接从进程分配内存， 而不是从JVM分配内存。
 * 分配给 RocksDB 的任何内存都必须被考虑在内，
 * 通常需要将这部分内存从任务管理器（TaskManager）的JVM堆中减去。
 * 不这样做可能会导致JVM进程由于分配的内存超过申请值而被 YARN/Mesos 等资源管理框架终止。
 *
 * 算控状态
 * Operator ID | State
 * ------------+------------------------
 * source-id   | State of StatefulSource
 * mapper-id   | State of StatefulMapper
 *
 * 目前每个算子的每个状态都在 RocksDB 中有专门的一个列族存储
 */
public class Flink06_State_RocksDB_Advance {
    public static void main(String[] args) {

    }
}
