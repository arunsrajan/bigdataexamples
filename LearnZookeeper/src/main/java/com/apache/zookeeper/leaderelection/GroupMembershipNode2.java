package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.retry.RetryForever;

public class GroupMembershipNode2 {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		GroupMember gm = new GroupMember(cf,"/datanode","localhost_9001","{\"NumShards\":3}".getBytes()) ;;
		gm.start();
		Thread.sleep(10000);
		System.out.println(gm.getCurrentMembers());
		gm.close();
		cf.close();

	}

}
