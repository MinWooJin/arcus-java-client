package net.spy.memcached;

import net.spy.memcached.compat.SpyThread;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MonitorServer extends SpyThread {
  private volatile boolean shutdownRequested = false;

  private ArcusClient[] client;
  private SystemMonitor monitor = null;
  private ExecutorService threadPool = Executors.newFixedThreadPool(10);
  private ServerSocket serverSocket = null;
  private final long serverDuration;
  private final int serverLevel;
  private final int serverPort;
  private Object notificationType;
  private OperatingSystemMXBean operatingSystemMXBean;
  private Method getProcessCpuLoadMethod;
  private Method getSystemCpuLoadMethod;


  public MonitorServer(ArcusClient[] c, long d, int l, boolean monitorServer, int p) {
    client = c;
    serverDuration = d;
    serverLevel = l;
    serverPort = p;

    GCMonitorSupportable();
    CpuMonitorSupportable();

    startMonitor(serverDuration, serverLevel);

    if (monitorServer) {
      openServerSocket();

      setName("MonitorServer");
      setDaemon(true);
      start();
    } else {
      /* monitor server disable. monitoring one time */
      shutdownRequested = true;
    }
  }

  public void GCMonitorSupportable() {
    try {
      Class<?> clazz = Class.forName("com.sun.management.GarbageCollectionNotificationInfo");

      Field notificationTypeField = clazz.getDeclaredField("GARBAGE_COLLECTION_NOTIFICATION");
      if (!Modifier.isPublic(notificationTypeField.getModifiers())) {
        throw new NotSupportException("Inaccessible notification type field.");
      }

      notificationType = notificationTypeField.get(clazz);
      if (notificationType == null) {
        throw new NotSupportException("Notification type is null.");
      }
    } catch (Exception e) { /* ClassNotFoundException | NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException */
      throw new NotSupportException(e);
    }
  }

  public void CpuMonitorSupportable() {
    operatingSystemMXBean = getOperatingSystemMXBean();

    try {
      getProcessCpuLoadMethod = operatingSystemMXBean.getClass().getDeclaredMethod("getProcessCpuLoad");
      getSystemCpuLoadMethod = operatingSystemMXBean.getClass().getDeclaredMethod("getSystemCpuLoad");
    } catch (NoSuchMethodException e) {
      throw new NotSupportException(e);
    }

    try {
      getProcessCpuLoadMethod.setAccessible(true);
      getSystemCpuLoadMethod.setAccessible(true);
    } catch (Exception e) {
      if (e.getClass().getName() == "java.lang.reflect.InaccessibleObjectException") {
        throw new NotSupportException(
                "if jdk version is 9 or higher, add the JVM argument '--add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED'\n" +
                        e.getClass().getName() + ":" + e.getMessage()
        );
      } else {
        throw new NotSupportException(e);
      }
    }

    if (getProcessCpuLoadMethod.getReturnType() != double.class || getSystemCpuLoadMethod.getReturnType() != double.class) {
      throw new NotSupportException("Return type of CPU load method is not double.class");
    }
  }

  private OperatingSystemMXBean getOperatingSystemMXBean() {
    OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    if (operatingSystemMXBean == null) {
      throw new NotSupportException("ManagementFactory.getOperatingSystemMXBean() is null");
    }
    return operatingSystemMXBean;
  }

  public void run() {
    while (!shutdownRequested) {
      try {
        Socket clientSocket = serverSocket.accept();
        threadPool.execute(new ProcessMonitorCommand(clientSocket.getInputStream(),
                clientSocket.getOutputStream(), this));
      } catch (IOException ex) {
        getLogger().warn("Error while running monitor server.. Shut down : %s", ex.getMessage());
        shutdown();
      }
    }
    closeServerSocket();
    getLogger().info("close Monitor Server");
  }

  private void openServerSocket() {
    try {
      serverSocket = new ServerSocket(serverPort);
    } catch (IOException e) {
      throw new RuntimeException("can't open connection on port:" + serverPort);
    }
  }

  private void closeServerSocket() {
    try {
      serverSocket.close();
      serverSocket = null;
    } catch (IOException e) {
      throw new RuntimeException("Error while closing server socket.");
    }
  }

  public boolean startMonitor(long d, int l) {
    if (d > 0) {
      synchronized (this) {
        if (monitor != null) {
          if (monitor.isShutdownRequested()) {
            monitor = null;
          } else {
            return false;
          }
        }

        monitor = new SystemMonitor(client, d, l, notificationType, operatingSystemMXBean, getProcessCpuLoadMethod, getSystemCpuLoadMethod);
      }
      return true;
    }
    return false;
  }

  public void stopMonitor() {
    synchronized (this) {
      if (monitor == null) {
        return;
      }

      monitor.shutdown();
      monitor = null;
    }
  }

  public SystemMonitor getMonitor() {
    synchronized (this) {
      if (monitor != null) {
        if (monitor.isShutdownRequested()) {
          monitor = null;
        }
      }

      return monitor;
    }
  }

  public long getDuration() {
    return serverDuration;
  }

  public int getLevel() {
    return serverLevel;
  }

  public void shutdown() {
    if (!shutdownRequested) {
      stopMonitor();
      getLogger().info("Shut down monitor server.");
      shutdownRequested = true;
    }
  }

  public static class NotSupportException extends RuntimeException {
    private static final long serialVersionUID = 8040767729768864994L;
    public NotSupportException(Exception e) {
      super(e.getClass().getName() + ":" + e.getMessage());
    }
    public NotSupportException(String message) {
      super(message);
    }
  }
}
