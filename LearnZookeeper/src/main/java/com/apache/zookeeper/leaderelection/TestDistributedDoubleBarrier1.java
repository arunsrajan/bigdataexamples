package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.RetryForever;

public class TestDistributedDoubleBarrier1 {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		
		DistributedDoubleBarrier ddb = new DistributedDoubleBarrier(cf,
                "/barrier",
                2);
		System.out.println("before1");
		ddb.enter();
		System.out.println("processing1");
		System.out.println("waiting to leave in node1");
		ddb.leave();
		System.out.println("leave1");
		cf.close();
	}

}
