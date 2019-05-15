package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.FSCachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.util.Time;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEEP_COPY_REPLICA_THRESHOLD_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.DEEP_COPY_REPLICA_THRESHOLD_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeys.REPLICA_CACHING_GET_SPACE_USED_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_KEY;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

@InterfaceAudience.LimitedPrivate({ "HDFS", "MapReduce" })
@InterfaceStability.Evolving
public class ReplicaCachingGetSpaceUsed extends FSCachingGetSpaceUsed {

    public static final Log LOG =  LogFactory.getLog(ReplicaCachingGetSpaceUsed.class);
    private FsVolumeImpl volume;
    private String bpid;
    private Configuration conf;
    private long deepCopyReplicaThresholdMs;
    private long replicaCachingGetSpaceUsedThresholdMs;

    public ReplicaCachingGetSpaceUsed(CachingGetSpaceUsed.Builder builder) throws IOException {
        super(builder);

    }

    public ReplicaCachingGetSpaceUsed(Builder builder) throws IOException {
        super(builder);
        volume = builder.getVolume();
        bpid = builder.getBpid();
        conf = builder.getConf();
        deepCopyReplicaThresholdMs = conf.getLong(DEEP_COPY_REPLICA_THRESHOLD_KEY,DEEP_COPY_REPLICA_THRESHOLD_DEFAULT);
        replicaCachingGetSpaceUsedThresholdMs = conf.getLong(REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_KEY,REPLICA_CACHING_GET_SPACE_USED_DEFAULT);

    }

    @Override
    protected void refresh(){
        long start = Time.monotonicNow();
        long dfsUsed = 0;
        long count = 0;

        FsDatasetSpi fsDataset = volume.getDataset();
        try{
            Collection<ReplicaInfo> replicaInfos = (Collection<ReplicaInfo>) fsDataset.deepCopyReplica(bpid);
            long cost = Time.monotonicNow() - start;
            if (cost > deepCopyReplicaThresholdMs) {
                LOG.info("blockPoolSlice: " + bpid + " replicas size:"
                        + replicaInfos.size() + " copy replicas duration: "
                        + (Time.monotonicNow() - start) + "ms");
            }
            if (CollectionUtils.isNotEmpty(replicaInfos)) {
                for (ReplicaInfo replicaInfo : replicaInfos) {
                    if (Objects.equals(replicaInfo.getVolume().getStorageID(),
                            volume.getStorageID())) {
                        dfsUsed += replicaInfo.getBytesOnDisk();
                        dfsUsed += replicaInfo.getMetadataLength();
                        count++;
                    }
                }

            }
            this.used.set(dfsUsed);
            cost = Time.monotonicNow() - start;
            if (cost > replicaCachingGetSpaceUsedThresholdMs) {
                LOG.info("refresh dfs used, bpid: " + bpid + " replicas size: " + count
                        + " dfsUsed: " + this.used + " on volume: " + volume.getStorageID()
                        + " duration: " + (Time.monotonicNow() - start) + "ms");
            }
        }catch (IOException e){
            LOG.error("replicaCachingGetSpaceUsed refresh error", e);
        }

    }
}
