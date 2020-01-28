<?php

namespace Container;

/**
 * @return object
 */
function getRedis(): object
{
    $host = getenv('REDIS_HOST');
    $port = getenv('REDIS_PORT');
    $persistent = false;
    return new \Ejz\RedisClient(compact('persistent', 'host', 'port'));
}

/**
 * @return object
 */
function getCache(): object
{
    return new \Ejz\RedisCache(getRedis());
}

/**
 * @return object
 */
function getBitmap(string $name): object
{
    $name = strtoupper($name);
    $host = getenv("BITMAP_{$name}_HOST") ?: getenv('BITMAP_HOST');
    $port = getenv("BITMAP_{$name}_PORT") ?: getenv('BITMAP_PORT');
    $persistent = false;
    $client = new \Ejz\RedisClient(compact('persistent', 'host', 'port'));
    return new \Ejz\Bitmap($name, $client);
}

/**
 * @param string $name
 *
 * @return object
 */
function getDatabase(string $name): object
{
    $name = strtoupper($name);
    $host = getenv("DB_{$name}_HOST") ?: getenv('DB_HOST');
    $port = getenv("DB_{$name}_PORT") ?: getenv('DB_PORT');
    $user = getenv("DB_{$name}_USER") ?: getenv('DB_USER');
    $password = getenv("DB_{$name}_PASSWORD") ?: getenv('DB_PASSWORD');
    $db = getenv("DB_{$name}_NAME") ?: getenv('DB_NAME');
    $dsn = "host={$host} port={$port} user={$user} password={$password} db={$db}";
    return new \Ejz\DatabasePostgres($name, $dsn);
}

/**
 * @return object
 */
function getDatabasePool(): object
{
    $envs = getenv('DB_ENVS');
    $instances = [];
    foreach (explode(',', $envs) as $env) {
        $instances[] = getDatabase(strtolower($env));
    }
    return new \Ejz\Pool($instances);
}

/**
 * @return object
 */
function getBitmapPool(): object
{
    $envs = getenv('BITMAP_ENVS');
    $instances = [];
    foreach (explode(',', $envs) as $env) {
        $instances[] = getBitmap(strtolower($env));
    }
    return new \Ejz\Pool($instances);
}

/**
 * @param array $repositories
 *
 * @return object
 */
function getStorage(array $repositories): object
{
    $databasePool = getDatabasePool();
    $bitmapPool = getBitmapPool();
    $cache = getCache();
    return new \Ejz\Storage($databasePool, $bitmapPool, $cache, $repositories);
}
