package net.spy.memcached;

import net.spy.memcached.compat.SpyThread;
import net.spy.memcached.ops.OperationMonitorWriter;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

public class SystemMonitor extends SpyThread {

  private volatile boolean shutdownRequested = false;
  private OperatingSystemMXBean operatingSystemMXBean = null;
  private ArcusClient[] client;
  private int level = 0;
  private long duration;
  private Map<NotificationEmitter, NotificationListener> gcbeans = null;
  private Object notificationType;
  private Method getProcessCpuLoadMethod;
  private Method getSystemCpuLoadMethod;

  public SystemMonitor(ArcusClient[] c, long d, int l, Object nt, OperatingSystemMXBean osm, Method pm, Method sm) {
    gcbeans = new HashMap<NotificationEmitter, NotificationListener>();
    client = c;
    duration = d;
    level = l;
    notificationType = nt;
    operatingSystemMXBean = osm;
    getProcessCpuLoadMethod = pm;
    getSystemCpuLoadMethod = sm;

    setName("SystemMonitor");
    setDaemon(true);
    start();
  }

  public void run() {
    synchronized (this) {
      long time = 0;
      if (level > 0) {
        runningOperationMonitor();
      }
      runningGCNotificator();
      while (!shutdownRequested) {
        try {
          double jvmCpuLoad = getProcessCpuLoad();
          double systemCpuLoad = getSystemCpuLoad();

          getLogger().warn("[ARCUS::MONITOR] {systemCpuLoad=" + (int)(systemCpuLoad*100) + "%"
                  + ", jvmCpuLoad=" + (int)(jvmCpuLoad*100) + "%" + "}");

          Thread.sleep(1000);
          time += 1;

          if (time >= duration) {
            shutdownRequested = true;
          }
        } catch (InterruptedException e) {
          getLogger().warn("SystemMonitor thread is interrupted while sleep: %s",
                  e.getMessage());
          shutdownRequested = true;
        }
      }
      stopOperationMonitor();
      stopGCNotificator();
    }
    getLogger().info("close system monitor");
  }

  public double getProcessCpuLoad() {
    Object result = null;

    try {
      result = getProcessCpuLoadMethod.invoke(operatingSystemMXBean);
    } catch (Exception e) { /* IllegalAccessException | IllegalArgumentException | InvocationTargetException */
      getLogger().warn("exception during getProcessCpuLoad");
    } finally {
      if (result == null) {
        result = (double) 0;
      }
    }
    return (Double) result;
  }

  public double getSystemCpuLoad() {
    Object result = null;

    try {
      result = getSystemCpuLoadMethod.invoke(operatingSystemMXBean);
    } catch (Exception e) { /* IllegalAccessException | IllegalArgumentException | InvocationTargetException */
      getLogger().warn("exception during getSystemCpuLoad");
    } finally {
      if (result == null) {
        result = (double) 0;
      }
    }
    return (Double) result;
  }

  public void runningOperationMonitor() {
    for (ArcusClient ac : client) {
      MemcachedConnection conn = ac.getMemcachedConnection();
      OperationMonitorWriter omw = new OperationMonitorWriter(conn, level);
      conn.setOperationMonitorWriter(omw);
    }
  }

  public void stopOperationMonitor() {
    for (ArcusClient ac : client) {
      MemcachedConnection conn = ac.getMemcachedConnection();
      OperationMonitorWriter omw = conn.getOperationMonitorWriter();
      if (omw != null) {
        omw.shutdownOperationWriter();
        conn.setOperationMonitorWriter(null);
      }
    }
  }

  public void runningGCNotificator() {
    // get all the GarbageCollectorMXBeans - there's one for each heap generation
    // so probably two - the old generation and young generation
    List<GarbageCollectorMXBean> gcbs = ManagementFactory.getGarbageCollectorMXBeans();
    // Install a notifcation handler for each bean
    for (GarbageCollectorMXBean gcbean : gcbs) {
      NotificationEmitter emitter = (NotificationEmitter) gcbean;
      // use an anonymously generated listener for this example
      // - proper code should really use a named class
      NotificationListener listener = new NotificationListener() {
        @Override
        public void handleNotification(Notification notification, Object handback) {
          if (notification.getType().equals(notificationType.toString())) {
            CompositeData userData, gcInfo;
            String gcAction, gcCause, gcName;
            long gcStartTime, gcDuration;
            Date realGcStartTime;

            try {
              userData = (CompositeData) notification.getUserData();
              gcAction = (String) userData.get("gcAction");
              gcCause = (String) userData.get("gcCause");
              gcName = (String) userData.get("gcName");
              gcInfo = (CompositeData) userData.get("gcInfo");
              gcStartTime = (Long) gcInfo.get("startTime");
              gcDuration = (Long) gcInfo.get("duration");
              realGcStartTime = new Date(gcStartTime + ManagementFactory.getRuntimeMXBean().getStartTime());
                /* gc timeStamp + jvm start timeStamp */

              getLogger().warn("[ARCUS::MONITOR] {%s: [%s (%s, %s) [%d msecs]]}",
                      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(realGcStartTime),
                      gcAction,
                      gcCause,
                      gcName,
                      gcDuration);
            } catch (Exception e) {
              getLogger().warn("Invalid GC notificationType.. stop gc monitoring");
              stopGCNotificator();
            }
          }
        }
      };
      // Add the listener
      emitter.addNotificationListener(listener, null, null);
      gcbeans.put(emitter, listener);
    }
    getLogger().info("set GCNotificator listener in SystemMonitor");
  }

  public void stopGCNotificator() {
    for (Map.Entry<NotificationEmitter, NotificationListener> m : gcbeans.entrySet()) {
      NotificationEmitter emitter = m.getKey();
      NotificationListener listener = m.getValue();
      try {
        emitter.removeNotificationListener(listener, null, null);
      } catch (ListenerNotFoundException e) {
        getLogger().warn("GCNotificator listener not found: %s",
                e.getMessage());
      }
    }
    getLogger().info("remove GCNotificator listener in SystemMonitor");
  }

  public boolean isShutdownRequested() {
    return shutdownRequested;
  }

  public void shutdown() {
    if (!shutdownRequested) {
      getLogger().info("Shut down system monitor.");
      shutdownRequested = true;
    }
  }
}
