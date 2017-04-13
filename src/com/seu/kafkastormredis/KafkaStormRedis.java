package com.seu.kafkastormredis;

import java.util.Arrays;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;

import com.seu.storm2redis.WordCounter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.ITuple;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaStormRedis {
	private static final String WORD_SPOUT = "WORD_SPOUT";
	private static final String COUNT_BOLT = "COUNT_BOLT";
	private static final String STORE_BOLT = "STORE_BOLT";
	private static final String TEST_REDIS_HOST = "192.168.1.113";
	private static final int TEST_REDIS_PORT = 6379;

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		// redis
		Config config = new Config();
		String host = TEST_REDIS_HOST;
		int port = TEST_REDIS_PORT;
		JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(host).setPort(port).build();
		WordCounter bolt = new WordCounter();
		RedisStoreMapper storeMapper = setupStoreMapper();
		RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);

		// kafka
		String zks = "hadoop02:2181,hadoop03:2181,hadoop04:2181";
		String topic = "wordscount";
		String zkRoot = "/storm"; // default zookeeper root configuration for
									// storm
		String id = "word";

		BrokerHosts brokerHosts = new ZkHosts(zks);
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		// spoutConf.forceFromStart = false;
		spoutConf.ignoreZkOffsets = true;
		spoutConf.zkServers = Arrays.asList(new String[] { "hadoop02", "hadoop03", "hadoop04" });
		spoutConf.zkPort = 2181;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(WORD_SPOUT, new KafkaSpout(spoutConf), 4);
		builder.setBolt(COUNT_BOLT, bolt, 4).shuffleGrouping(WORD_SPOUT);
		builder.setBolt(STORE_BOLT, storeBolt, 4).shuffleGrouping(COUNT_BOLT);

		
		Config conf = new Config();
		conf.setNumWorkers(4);
		conf.setNumAckers(0);
		conf.setDebug(false);
		StormSubmitter.submitTopology("wordscount", conf, builder.createTopology());
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("wordscount", config, builder.createTopology());
//		Thread.sleep(30000);
//		cluster.killTopology("wordscount");
//		cluster.shutdown();
//		System.exit(0);
	}

	private static RedisStoreMapper setupStoreMapper() {
		return new WordCountStoreMapper();
	}

	private static class WordCountStoreMapper implements RedisStoreMapper {
		private RedisDataTypeDescription description;
		private final String hashKey = "wordCount";

		public WordCountStoreMapper() {
			description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
		}

		@Override
		public RedisDataTypeDescription getDataTypeDescription() {
			return description;
		}

		@Override
		public String getKeyFromTuple(ITuple tuple) {
			return tuple.getStringByField("word");
		}

		@Override
		public String getValueFromTuple(ITuple tuple) {
			return tuple.getStringByField("count");
		}
	}
}
