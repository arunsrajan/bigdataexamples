package com.apache.zookeeper.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryForever;

public class TestDistributedLock {

	public static void main(String[] args) throws Exception {
		CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", new RetryForever(2000));
		cf.start();
		InterProcessSemaphoreMutex ipsm = new InterProcessSemaphoreMutex(cf,"/curator/test");
		ipsm.acquire();
		System.out.println("Lock1 acquired");
		Thread.sleep(10000);
		ipsm.release();
		System.out.println("Lock1 released");
		cf.close();
	}

}
