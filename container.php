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
function getBitmap(): object
{
    $host = getenv('BITMAP_HOST');
    $port = getenv('BITMAP_PORT');
    $persistent = false;
    $client = new \Ejz\RedisClient(compact('persistent', 'host', 'port'));
    return new \Ejz\Bitmap($client);
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
    return new \Ejz\DatabaseExtendedPostgres($name, $dsn);
}

/**
 * @return object
 */
function getDatabasePool(): object
{
    $envs = getenv('DB_ENVS');
    $dbs = [];
    foreach (explode(',', $envs) as $env) {
        $dbs[] = getDatabase(strtolower($env));
    }
    return new \Ejz\DatabasePool($dbs);
}

/**
 * @param array $repositories
 *
 * @return object
 */
function getStorage(array $repositories): object
{
    $pool = getDatabasePool();
    $cache = getCache();
    $bitmap = getBitmap();
    return new \Ejz\Storage($pool, $cache, $bitmap, $repositories);
}
