package org.apache.spark.streaming.kafka;

import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction;



public class ShardAlg {/*

	
	

    public static int shard(String id, int shardNum) {
        int hash = Murmur3HashFunction.hash(id);
        return mod(hash, shardNum);
    }

    public static int mod(int v, int m) {
        int r = v % m;
        if (r < 0) {
            r += m;
        }
        return r;
    }
*/}
