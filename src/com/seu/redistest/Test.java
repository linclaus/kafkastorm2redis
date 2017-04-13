package com.seu.redistest;

import java.util.Set;

import redis.clients.jedis.Jedis;

public class Test {
	public static void main(String[] args) {
		Jedis jedis=new Jedis("192.168.1.113");
		Set<String> zrange = jedis.zrange("c", 0, -1);
		System.out.println(zrange.toString());
	}
}
