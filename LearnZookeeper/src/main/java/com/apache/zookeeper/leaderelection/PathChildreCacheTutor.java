package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryForever;

public class PathChildreCacheTutor {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		PathChildrenCache pcc = new PathChildrenCache(cf,"/pathchild",true);
		pcc.getListenable().addListener(new PathChildrenCacheListener () {

			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				switch(event.getType()) {
				case CHILD_REMOVED:
					System.out.println("Child Removed = "+event.getData().getPath());
					break;
				case CHILD_ADDED:
					System.out.println("Child ADDED = "+event.getData().getPath());
					break;
				case CHILD_UPDATED:
					System.out.println("Child Updated = "+event.getData().getPath());
					break;
					
				}
				
			}
			
		});
		pcc.start();
		Thread.sleep(400000);
		pcc.close();
		cf.close();
	}

}
