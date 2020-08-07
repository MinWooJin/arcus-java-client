package net.spy.memcached.internal;

/**
 * The type of watching znode.
 */
public enum ZnodeType {
  CacheList,
  AlterList,
  Migrations,
  MigrationVersion,
  MigrationInit
}
