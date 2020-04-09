package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryForever;

public class TestLeaderElection2 {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		LeaderLatch ll = new LeaderLatch(cf, "/curator/test");
		ll.addListener(new LeaderLatchListener() {

			public void isLeader() {
				System.out.println("Leader2");
			}

			public void notLeader() {
				
			}
			
		});
		ll.start();
		Thread.sleep(60000);
		ll.close();
		cf.close();
	}

}
