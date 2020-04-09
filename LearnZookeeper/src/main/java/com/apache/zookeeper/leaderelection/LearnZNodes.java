package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;

public class LearnZNodes {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/treecache/Create","data".getBytes());
		cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/treecache/Create/ephe","data".getBytes());
		Thread.sleep(5000);
		cf.setData().forPath("/treecache/Create","data1".getBytes());
		Thread.sleep(10000);
		cf.close();
	}

}
