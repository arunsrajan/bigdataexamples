package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.retry.RetryForever;

public class GroupMembershipNode1 {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		GroupMember gm = new GroupMember(cf,"/datanode","localhost_9000","{\"NumShards\":2}".getBytes());;
		gm.start();
		System.out.println(gm.getCurrentMembers());
		Thread.sleep(50000);
		gm.close();
		cf.close();
	}

}
