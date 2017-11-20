/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.NavigableSet;
import java.util.NavigableMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.MigrationMode;
import net.spy.memcached.util.ArcusKetamaNodeLocatorConfiguration;

public class ArcusKetamaNodeLocator extends SpyObject implements NodeLocator {

  TreeMap<Long, MemcachedNode> ketamaNodes;
  Collection<MemcachedNode> allNodes;

  /* ENABLE_MIGRATION if */
  TreeMap<Long, MemcachedNode> migrationKetamaNodes;
  Collection<MemcachedNode> allExistNodes;
  Collection<MemcachedNode> allAlterNodes;
  Collection<MemcachedNode> allFailedExistNodes;
  Collection<MemcachedNode> allFailedAlterNodes;
  HashMap<MemcachedNode, List<Long>> allHashPoints;
  Collection<MemcachedNode> allMigrationNodes;
  List<String> sortedExistNames;
  int migrationExecutionRound = 0;
  MigrationMode migrationMode = MigrationMode.Init;
  String prevExistNodeName = null;
  int prevExistHSliceIndex = -1;
  /* ENABLE_MIGRATION end */

  HashAlgorithm hashAlg;
  ArcusKetamaNodeLocatorConfiguration config;

  Lock lock = new ReentrantLock();

  public ArcusKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg) {
    this(nodes, alg, new ArcusKetamaNodeLocatorConfiguration());
  }

  public ArcusKetamaNodeLocator(List<MemcachedNode> nodes, HashAlgorithm alg,
                                ArcusKetamaNodeLocatorConfiguration conf) {
    super();
    allNodes = nodes;
    hashAlg = alg;
    ketamaNodes = new TreeMap<Long, MemcachedNode>();
    config = conf;

    /* ENABLE_MIGRATION if */
    migrationKetamaNodes = new TreeMap<Long, MemcachedNode>();
    allMigrationNodes = new ArrayList<MemcachedNode>();
    allExistNodes = new ArrayList<MemcachedNode>();
    allFailedExistNodes = new ArrayList<MemcachedNode>();
    allAlterNodes = new ArrayList<MemcachedNode>();
    allFailedAlterNodes = new ArrayList<MemcachedNode>();
    allHashPoints = new HashMap<MemcachedNode, List<Long>>();
    sortedExistNames = new ArrayList<String>();
    /* ENABLE_MIGRATION end */

    int numReps = config.getNodeRepetitions();
    for (MemcachedNode node : nodes) {
      // Ketama does some special work with md5 where it reuses chunks.
      if (alg == HashAlgorithm.KETAMA_HASH) {
        updateHash(node, false);
      } else {
        for (int i = 0; i < numReps; i++) {
          ketamaNodes.put(
                  hashAlg.hash(config.getKeyForNode(node, i)), node);
        }
      }

      /* ENABLE_MIGRATION if */
      allHashPoints.put(node, prepareHashPoint(node));
      /* ENABLE_MIGRATION end */
    }
    assert ketamaNodes.size() == numReps * nodes.size();
  }

  private ArcusKetamaNodeLocator(TreeMap<Long, MemcachedNode> smn,
                                 Collection<MemcachedNode> an, HashAlgorithm alg,
                                 ArcusKetamaNodeLocatorConfiguration conf) {
    super();
    ketamaNodes = smn;
    allNodes = an;
    hashAlg = alg;
    config = conf;
  }

  public Collection<MemcachedNode> getAll() {
    return allNodes;
  }


  /* ENABLE_MIGRATION if */
  public Collection<MemcachedNode> getAllMigrationNodes() {
    return allMigrationNodes;
  }
  /* ENABLE_MIGRATION end */

  public MemcachedNode getPrimary(final String k) {
    MemcachedNode rv = getNodeForKey(hashAlg.hash(k));
    assert rv != null : "Found no node for key " + k;
    return rv;
  }

  long getMaxKey() {
    return ketamaNodes.lastKey();
  }

  MemcachedNode getNodeForKey(long hash) {
    MemcachedNode rv;

    lock.lock();
    try {
      if (!ketamaNodes.containsKey(hash)) {
        Long nodeHash = ketamaNodes.ceilingKey(hash);
        if (nodeHash == null) {
          hash = ketamaNodes.firstKey();
        } else {
          hash = nodeHash.longValue();
        }
        // Java 1.6 adds a ceilingKey method, but I'm still stuck in 1.5
        // in a lot of places, so I'm doing this myself.
        /*
        SortedMap<Long, MemcachedNode> tailMap = ketamaNodes.tailMap(hash);
        if (tailMap.isEmpty()) {
          hash = ketamaNodes.firstKey();
        } else {
          hash = tailMap.firstKey();
        }
        */
      }
      /* ENABLE_MIGRATION if */
      if (allFailedExistNodes.isEmpty()) {
        rv = ketamaNodes.get(hash);
      } else {
        do {
          rv = ketamaNodes.get(hash);
          Long nodeHash = ketamaNodes.higherKey(hash);
          if (nodeHash == null) {
            hash = ketamaNodes.firstKey();
          } else {
            hash = nodeHash.longValue();
          }
        } while (allFailedExistNodes.contains(rv));
      }
      /* else */
      /*
      rv = ketamaNodes.get(hash);
      */
      /* ENABLE_MIGRATION end */
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }
    return rv;
  }

  public Iterator<MemcachedNode> getSequence(String k) {
    return new KetamaIterator(k, allNodes.size());
  }

  public NodeLocator getReadonlyCopy() {
    TreeMap<Long, MemcachedNode> smn = new TreeMap<Long, MemcachedNode>(
            ketamaNodes);
    Collection<MemcachedNode> an = new ArrayList<MemcachedNode>(
            allNodes.size());

    lock.lock();
    try {
      // Rewrite the values a copy of the map.
      for (Map.Entry<Long, MemcachedNode> me : smn.entrySet()) {
        me.setValue(new MemcachedNodeROImpl(me.getValue()));
      }
      // Copy the allNodes collection.
      for (MemcachedNode n : allNodes) {
        an.add(new MemcachedNodeROImpl(n));
      }
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }

    return new ArcusKetamaNodeLocator(smn, an, hashAlg, config);
  }

  public void update(Collection<MemcachedNode> toAttach,
                     Collection<MemcachedNode> toDelete) {
    lock.lock();
    try {
      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        allNodes.add(node);
        updateHash(node, false);
        /* ENABLE_MIGRATION if */
        allHashPoints.put(node, prepareHashPoint(node));
        /* ENABLE_MIGRATION end */
      }

      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allNodes.remove(node);
        /* ENABLE_MIGRATION if */
        if (migrationMode == MigrationMode.Init) {
          allHashPoints.remove(node);
          updateHash(node, true);
        } else if (migrationMode == MigrationMode.Join) {
          if (!allFailedExistNodes.contains(node)) {
            allFailedExistNodes.add(node);
          }
          allExistNodes.remove(node);
          /* TODO::FIXME::sorted exist node use for same ordering of server */
          sortedExistNames.remove(node.getSocketAddress().toString());
          allHashPoints.remove(node);
          updateHash(node, true);
        } else {
          assert migrationMode == MigrationMode.Leave;

          if (allAlterNodes.contains(node)) {
            allHashPoints.remove(node);
            allFailedAlterNodes.add(node);
            updateHash(node, true);
          } else {
            if (allFailedAlterNodes.contains(node)) {
              /* do nothing */
            } else {
              allFailedExistNodes.add(node);
            }
          }
        }
        /* else */
        /*
        updateHash(node, true);
        */
        /* ENABLE_MIGRATION end */

        try {
          node.getSk().attach(null);
          node.shutdown();
        } catch (IOException e) {
          getLogger().error(
                  "Failed to shutdown the node : " + node.toString());
          node.setSk(null);
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }
  }

  private void updateHash(MemcachedNode node, boolean remove) {
    if (!remove) {
      config.insertNode(node);
    }

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {

      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);

        if (remove) {
          /* ENABLE_MIGRATION if */
          /* auto complete */
          if (migrationMode == MigrationMode.Join) {
            if (!migrationKetamaNodes.isEmpty()) {
              Long prev_k = ketamaNodes.lowerKey(k);
              NavigableMap<Long, MemcachedNode> sub;
              if (prev_k == null) {
                /* Ketama Hash point Tree map
                   |            | 0'th | ... | 159'th |              |
                   Migration Ketama Hash point Tree map
                   | alter 0'th |      | ... |        | alter 159'th |
                */
                prev_k = ketamaNodes.lastKey();
                NavigableMap<Long, MemcachedNode> temp = new TreeMap<Long, MemcachedNode>();
                sub = migrationKetamaNodes.subMap((long) 0, true, k, false);
                for (Map.Entry<Long, MemcachedNode> entry : sub.entrySet()) {
                  temp.put(entry.getKey(), entry.getValue());
                }
                if (prev_k < migrationKetamaNodes.lastKey()) {
                  sub = migrationKetamaNodes.subMap(prev_k, false, migrationKetamaNodes.lastKey(), true);
                  for (Map.Entry<Long, MemcachedNode> entry : sub.entrySet()) {
                    temp.put(entry.getKey(), entry.getValue());
                  }
                }
                sub = temp;
              } else {
                sub = migrationKetamaNodes.subMap(prev_k, false, k, false);
              }
              if (!sub.isEmpty()) {
                Map<Long, MemcachedNode> temp = new TreeMap<Long, MemcachedNode>();
                for (Map.Entry<Long, MemcachedNode> entry : sub.entrySet()) {
                  temp.put(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<Long, MemcachedNode> entry : temp.entrySet()) {
                  ketamaNodes.put(entry.getKey(), entry.getValue());
                  migrationKetamaNodes.remove(entry.getKey());
                }
              }
            }
          }

          /* ENABLE_MIGRATION end */
          ketamaNodes.remove(k);
        } else {
          ketamaNodes.put(k, node);
        }
      }
    }

    if (remove) {
      config.removeNode(node);
    }
  }

  /* ENABLE_MIGRATION if */
  public void cleanupMigration() {
    if (!allExistNodes.isEmpty()) {
      allExistNodes.clear();
    }
    if (!allAlterNodes.isEmpty()) {
      allAlterNodes.clear();
    }
    if (!allFailedExistNodes.isEmpty()) {
      lock.lock();
      for (MemcachedNode node : allFailedExistNodes) {
        updateHash(node, true);
      }
      lock.unlock();
      allFailedExistNodes.clear();
    }
    if (!allFailedAlterNodes.isEmpty()) {
      allFailedAlterNodes.clear();
    }
    if (!allMigrationNodes.isEmpty()) {
      allMigrationNodes.clear();
    }
    if (!migrationKetamaNodes.isEmpty()) {
      migrationKetamaNodes.clear();
    }
    if (!sortedExistNames.isEmpty()) {
      sortedExistNames.clear();
    }
    migrationExecutionRound = 0;
    prevExistNodeName = null;
    prevExistHSliceIndex = -1;
    migrationMode = MigrationMode.Init;

    getLogger().info("Cleanup Migration");
  }

  public MigrationMode getMigrationMode() {
    return migrationMode;
  }

  private void updateMigrationHash(MemcachedNode node, boolean remove) {
    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {

      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));
      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);

        if (remove) {
          ketamaNodes.remove(k);
          migrationKetamaNodes.remove(k);
        } else {
          migrationKetamaNodes.put(k, node);
        }
      }
    }
  }

  public void updateMigration(Collection<MemcachedNode> toAttach,
                              Collection<MemcachedNode> toDelete,
                              MigrationMode mode) {
    lock.lock();
    try {
      // Add memcached nodes.
      for (MemcachedNode node : toAttach) {
        if (node == null)
          continue;
        allMigrationNodes.add(node);
        if (mode == MigrationMode.Join) {
          updateMigrationHash(node, false);
        }
      }

      // Remove memcached nodes.
      for (MemcachedNode node : toDelete) {
        allMigrationNodes.remove(node);
        if (!allNodes.contains(node)) {
          updateMigrationHash(node, true);
          try {
            node.getSk().attach(null);
            node.shutdown();
          } catch (IOException e) {
            getLogger().error(
                    "Failed to shutdown the node : " + node.toString());
            node.setSk(null);
          }
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } finally {
      lock.unlock();
    }
  }

  public void reflectMigratedHash(List<String> migraitons) {


  }

  private List<Long> prepareHashPoint(MemcachedNode node) {
    // Ketama does some special work with md5 where it reuses chunks.
    List<Long> result = new ArrayList<Long>();
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(config.getKeyForNode(node, i));

      for (int h = 0; h < 4; h++) {
        Long k = ((long) (digest[3 + h * 4] & 0xFF) << 24)
                | ((long) (digest[2 + h * 4] & 0xFF) << 16)
                | ((long) (digest[1 + h * 4] & 0xFF) << 8)
                | (digest[h * 4] & 0xFF);
        result.add(k);
      }
    }
    Collections.sort(result);
    return result;
  }
  /* ENABLE_MIGRATION end */

  class KetamaIterator implements Iterator<MemcachedNode> {

    final String key;
    long hashVal;
    int remainingTries;
    int numTries = 0;

    public KetamaIterator(final String k, final int t) {
      super();
      hashVal = hashAlg.hash(k);
      remainingTries = t;
      key = k;
    }

    private void nextHash() {
      long tmpKey = hashAlg.hash((numTries++) + key);
      // This echos the implementation of Long.hashCode()
      hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
      hashVal &= 0xffffffffL; /* truncate to 32-bits */
      remainingTries--;
    }

    public boolean hasNext() {
      return remainingTries > 0;
    }

    public MemcachedNode next() {
      try {
        return getNodeForKey(hashVal);
      } finally {
        nextHash();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException("remove not supported");
    }

  }
}
