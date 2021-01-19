package com.ecust.checkpoint.rocksdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.util.Collection;

/**
 * @author JueQian
 * @create 01-19 14:55
 * RocksDB的配置
 */
public class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {

    private static final long DEFAULT_SIZE = 256 * 1024 * 1024;  // 256 MB
    private long blockCacheSize = DEFAULT_SIZE;

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setIncreaseParallelism(4)
                .setUseFsync(false);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setTableFormatConfig(
                new BlockBasedTableConfig()
                        .setBlockCacheSize(blockCacheSize)
                        .setBlockSize(128 * 1024));            // 128 KB
    }

    @Override
    public RocksDBOptionsFactory configure(Configuration configuration) {
        this.blockCacheSize =
                configuration.getLong("my.custom.rocksdb.block.cache.size", DEFAULT_SIZE);
        return this;
    }
}