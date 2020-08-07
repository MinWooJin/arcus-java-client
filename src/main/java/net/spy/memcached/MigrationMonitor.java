/*
 * arcus-java-client : Arcus Java client
 * Copyright 2017 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.MigrationMode;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class MigrationMonitor extends SpyObject {
  private final static String joiningListPath = "joining_list";

  private final static String leavingListPath = "leaving_list";

  private final static String migrationsPath = "INTERNAL/migrations";

  private final ZooKeeper zk;

  private final String cloudStatZPath;

  private final String serviceCode;

  private volatile boolean dead;

  private final MigrationMonitorListener listener;

  private final MigrationWatcher cloudStatWatcher;

  private final MigrationWatcher alterListWatcher;

  private final MigrationWatcher migrationsWatcher;

  private MigrationMode mode;

  /**
   * Constructor
   *
   * @param zk
   *            ZooKeeper connection
   * @param cloudStatZPath
   *            ZooKeeper cloud_stat directory path
   * @param serviceCode
   *            service code (or cloud name) to identify each cloud
   * @param listener
   *            Callback listener
   */
  public MigrationMonitor(ZooKeeper zk, final String cloudStatZPath, String serviceCode,
                          final MigrationMonitorListener listener) {
    this.zk = zk;
    this.cloudStatZPath = cloudStatZPath;
    this.serviceCode = serviceCode;
    this.listener = listener;
    this.cloudStatWatcher = new MigrationWatcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
          asyncGetCloudStat();
        }
      }

      @Override
      public void processResult(int rc, String s, Object o, List<String> list) {
        Code code = Code.get(rc);
        switch (code) {
          case OK:
            //set a new Watcher for joining list or leaving list
            setMigrationMode(list);
            break;
          case NONODE:
            getLogger().warn("Cloud_stat zpath is deleted" + getInfo());
            /* FIXME::CloudStat directory removed */
            shutdown();
            break;
          case SESSIONEXPIRED:
            getLogger().warn("cloudStatWatcher: Session expired. " +
                    "Trying to reconnect to the Arcus admin " + getInfo());
            shutdown();
            break;
          case NOAUTH:
            getLogger().fatal("cloudStatWatcher: Authorization failed " + getInfo());
            shutdown();
            break;
          case CONNECTIONLOSS:
            getLogger().warn("cloudStatWatcher: Connection lost. " +
                    "Trying to reconnect to the Arcus admin." + getInfo());
            asyncGetCloudStat();
            break;
          default:
            getLogger().warn("cloudStatWatcher:" +
                    "Ignoring an unexpected event from the Arcus admin." +
                    " code=" + code + ", " + getInfo());
            asyncGetCloudStat();
            break;
        }
      }
    };
    this.alterListWatcher = new MigrationWatcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
          asyncGetAlterList();
        }
      }

      @Override
      public void processResult(int rc, String s, Object o, List<String> list) {
        Code code = Code.get(rc);
        switch (code) {
          case OK:
            listener.commandAlterListChange(list, mode);
            break;
          case NONODE:
            /* handled by cloudStatWatcher when alter list znode removed */
            break;
          case SESSIONEXPIRED:
            getLogger().warn("alterListWatcher: Session expired. " +
                    "Trying to reconnect to the Arcus admin. " + getInfo());
            shutdown();
            break;
          case NOAUTH:
            getLogger().fatal("alterListWatcher: Authorization failed " + getInfo());
            shutdown();
            break;
          case CONNECTIONLOSS:
            getLogger().warn("alterListWatcher: Connection lost. " +
                    "Trying to reconnect to the Arcus admin." + getInfo());
            asyncGetAlterList();
            break;
          default:
            getLogger().warn("alterListWatcher:" +
                    " Ignoring an unexpected event from the Arcus admin." +
                    " code=" + code + ", " + getInfo());
            asyncGetAlterList();
            break;
        }
      }
    };
    this.migrationsWatcher = new MigrationWatcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
          asyncGetMigrationsList();
        }
      }

      @Override
      public void processResult(int rc, String s, Object o, List<String> list) {
        Code code = Code.get(rc);
        switch (code) {
          case OK:
            listener.commandMigrationsChange(list);
            break;
          case NONODE:
            /* FIXME::processing is required depending on the node's logic */
            if (mode != MigrationMode.Init) {
              mode = MigrationMode.Init;
              listener.initialMigration(mode);
            }
            break;
          case SESSIONEXPIRED:
            getLogger().warn("migrationsWatcher: Session expired." +
                    "Trying to reconnect to the Arcus admin. " + getInfo());
            shutdown();
            break;
          case NOAUTH:
            getLogger().fatal("migrationsWatcher: Authorization failed " + getInfo());
            shutdown();
            break;
          case CONNECTIONLOSS:
            getLogger().warn("migrationsWatcher: Connection lost. " +
                    "Trying to reconnect to the Arcus admin." + getInfo());
            asyncGetMigrationsList();
            break;
          default:
            getLogger().warn("migrationsWatcher:" +
                    " Ignoring an unexpected event from the Arcus admin." +
                    " code=" + code + ", " + getInfo());
            asyncGetMigrationsList();
            break;
        }
      }
    };
    getLogger().info("Initializing the MigrationMonitor");
    mode = MigrationMode.Init;
    asyncGetCloudStat();
  }

  public interface MigrationMonitorListener {
    void commandAlterListChange(List<String> children, MigrationMode mode);

    void commandMigrationsChange(List<String> children);

    void initialMigration(MigrationMode mode);

    void commandMigrationVersionChange(long version);

    void closing();
  }

  private interface MigrationWatcher extends Watcher, AsyncCallback.ChildrenCallback {}

  private void setMigrationMode(List<String> children) {
    if (children.size() == 3) {
      boolean prepared = false;
      boolean done = false;
      long clusterVersion = -1;

      /* STATE znode format
        - STATE^BEGIN
        - STATE^PREPARED^preparedVersion
        - STATE^DONE^doneVersion
       */
      for (String str : children) {
        if (str.startsWith("STATE")) {
          String[] split = str.split("\\^");
          if (split.length >= 2) {
            if (split[1].length() == 8 && split[1].equals("PREPARED")) {
              clusterVersion = Long.parseLong(split[3]);
              prepared = true;
              break;
            } else if (split[1].length() == 4 && split[1].equals("DONE")) {
              clusterVersion = Long.parseLong(split[2]);
              done = true;
              break;
            }
          }
        }
      }

      if (prepared) {
        if (children.contains(joiningListPath)) {
          mode = MigrationMode.Join;
          asyncGetAlterList();
          asyncGetMigrationsList();
        } else if (children.contains(leavingListPath)) {
          mode = MigrationMode.Leave;
          asyncGetAlterList();
          asyncGetMigrationsList();
        } else {
          /* FIXME::handling when AlterList directory error */
          getLogger().fatal("Migration alterList ZK directory error.");
          shutdown();
          return;
        }
        getLogger().info("Migration is prepared.");
        assert clusterVersion != -1;
        listener.commandMigrationVersionChange(clusterVersion);
      }

      if (done) {
        if (mode != MigrationMode.Init) {
          assert clusterVersion != -1;

          mode = MigrationMode.Init;
          listener.initialMigration(mode);
          listener.commandMigrationVersionChange(clusterVersion);
        }
      }
    } else {
      /* FIXME::processing is required depending on the node's logic */
      if (mode != MigrationMode.Init) {
        mode = MigrationMode.Init;
        listener.initialMigration(mode);
      }
    }
  }

  /**
   * Get the cloud stat asynchronously from the Arcus admin.
   */
  private void asyncGetCloudStat() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Set a new watch on " + (cloudStatZPath + serviceCode) + " for Migration");
    }
    zk.getChildren(cloudStatZPath + serviceCode, cloudStatWatcher, cloudStatWatcher, null);
  }

  /**
   * Get the alter list asynchronously from the Arcus admin.
   */
  private void asyncGetAlterList() {
    if (mode == MigrationMode.Join) {
      zk.getChildren(cloudStatZPath + serviceCode + "/" +
              joiningListPath, alterListWatcher, alterListWatcher, null);
    } else if (mode == MigrationMode.Leave) {
      zk.getChildren(cloudStatZPath + serviceCode + "/" +
              leavingListPath, alterListWatcher, alterListWatcher, null);
    } else {
      assert mode == MigrationMode.Init;
      /* do nothing */
    }
  }

  /**
   * Get the migrations list asynchronously from the Arcus admin.
   */
  private void asyncGetMigrationsList() {
    zk.getChildren(cloudStatZPath + serviceCode + "/" +
            migrationsPath, migrationsWatcher, migrationsWatcher, null);
  }

  /**
   * Shutdown the MigrationMonitor.
   */
  public void shutdown() {
    if (!dead) {
      getLogger().info("Shutting down the MigrationMonitor. " + getInfo());
      dead = true;
      listener.closing();
    }
  }

  /**
   * Check if the migration monitor is dead.
   */
  public boolean isDead() {
    return dead;
  }

  private String getInfo() {
    String zkSessionId = null;
    if(zk != null) {
      zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
    }

    return "[serviceCode=" + serviceCode + ", adminSessionId=" + zkSessionId + "]";
  }
}
