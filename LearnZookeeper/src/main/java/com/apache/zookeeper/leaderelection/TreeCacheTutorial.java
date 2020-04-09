package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryForever;

public class TreeCacheTutorial {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		TreeCache tc = new TreeCache(cf,"/treecache");
		tc.getListenable().addListener(new TreeCacheListener() {

			public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
				switch(event.getType()) {
				case NODE_REMOVED:
					System.out.println("NODE Removed = "+event.getData().getPath());
					break;
				case NODE_ADDED:
					System.out.println("NODE ADDED = "+event.getData().getPath());
					break;
				case NODE_UPDATED:
					System.out.println("NODE Updated = "+event.getData().getPath());
					break;
					
				}
				
			}
			
		});
		tc.start();
		Thread.sleep(700000);
		tc.close();
		cf.close();
	}

}
