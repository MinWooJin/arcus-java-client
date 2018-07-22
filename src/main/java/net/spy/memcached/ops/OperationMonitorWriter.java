package net.spy.memcached.ops;

import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.compat.SpyThread;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class OperationMonitorWriter extends SpyThread {
  private volatile boolean shutdown = false;
  private final static int MAX_QUEUE_SIZE = 100;

  private MemcachedConnection conn;
  private int level;
  private BlockingQueue<OperationMonitor> opMonitorQueue = new LinkedBlockingQueue<OperationMonitor>(MAX_QUEUE_SIZE);

  public OperationMonitorWriter(MemcachedConnection c, int l) {
    conn = c;
    level = l;
    setName("OperationMonitorWriter");
    setDaemon(true);
    start();
  }

  public void shutdownOperationWriter() {
    conn.setOperationMonitorWriter(null);
    shutdown = true;
  }

  public void putOpertionMonitor(OperationMonitor om) {
    if (!shutdown) {
      boolean retry;
      do {
        try {
          if (level == 1) {
            opMonitorQueue.offer(om);
          } else {
            opMonitorQueue.put(om);
          }
          retry = false;
        } catch (InterruptedException e) {
          assert level == 2;
          retry = true;
        }
      } while (retry);
    }
  }

  public void run() {
    while (!shutdown) {
      try {
        // Get Operation Monitor from the queue
        OperationMonitor om = opMonitorQueue.take();
        om.write();
      } catch (InterruptedException e) {
        shutdown = true;
      }
    }
  }
}
