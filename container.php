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
 * @param string $name
 *
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
    $dsn = "host={$host} port={$port} user={$user} password={$password} dbname={$db}";
    return new \Ejz\DatabasePostgres($name, $dsn);
}

/**
 * @param string $name
 * @param array  $config
 *
 * @return object
 */
function getRepository(string $name, array $config): object
{
    $name = strtolower($name);
    return new \Ejz\Repository(
        $name,
        $config,
        getDatabasePool(),
        getBitmapPool(),
        getCache()
    );
}

/**
 * @return object
 */
function getDatabasePool(): object
{
    $envs = getenv('DB_ENVS');
    $objects = [];
    foreach (explode(',', $envs) as $env) {
        $objects[] = getDatabase(strtolower($env));
    }
    return new \Ejz\DatabasePool($objects);
}

/**
 * @return object
 */
function getBitmapPool(): object
{
    $envs = getenv('BITMAP_ENVS');
    $objects = [];
    foreach (explode(',', $envs) as $env) {
        $objects[] = getBitmap(strtolower($env));
    }
    return new \Ejz\BitmapPool($objects);
}

/**
 * @param array $configs
 *
 * @return object
 */
function getRepositoryPool(array $configs): object
{
    $objects = [];
    foreach ($configs as $name => $config) {
        $objects[] = getRepository($name, $config);
    }
    return new \Ejz\RepositoryPool($objects);
}
