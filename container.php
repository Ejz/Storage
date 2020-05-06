<?php

namespace Container;

/**
 * @param string $class
 * @param array  ...$args
 *
 * @return mixed
 */
function get(string $class, ...$args)
{
    switch ($class) {
        case \Ejz\RedisClient::class:
            $host = getenv('REDIS_HOST');
            $port = getenv('REDIS_PORT');
            $persistent = false;
            return new \Ejz\RedisClient(compact('persistent', 'host', 'port'));
        case \Ejz\RedisCache::class:
            return new \Ejz\RedisCache(get(\Ejz\RedisClient::class));
        case \Ejz\Database::class:
            [$name] = $args;
            $name = strtoupper($name);
            $host = getenv("DATABASE_{$name}_HOST") ?: getenv('DATABASE_HOST');
            $port = getenv("DATABASE_{$name}_PORT") ?: getenv('DATABASE_PORT');
            $user = getenv("DATABASE_{$name}_USER") ?: getenv('DATABASE_USER');
            $password = getenv("DATABASE_{$name}_PASSWORD") ?: getenv('DATABASE_PASSWORD');
            $db = getenv("DATABASE_{$name}_NAME") ?: getenv('DATABASE_NAME');
            $dsn = "host={$host} port={$port} user={$user} password={$password} dbname={$db}";
            $config = [];
            if (getenv('DEBUG')) {
                $config['logger'] = function ($sql, $args, $result) {
                    echo ($l = str_repeat('-', 10) . date(' H:i:s ') . str_repeat('-', 10)), "\n";
                    echo trim($sql), "\n";
                    echo json_encode($args, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE), "\n";
                    echo json_encode($result, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE), "\n";
                    echo str_repeat('-', strlen($l)), "\n";
                };
            }
            return new \Ejz\Database($name, $dsn, $config);
        case \Ejz\DatabasePool::class:
            $pool = getenv('DATABASE_POOL');
            $objects = [];
            foreach (explode(',', $pool) as $name) {
                $objects[] = get(\Ejz\Database::class, strtolower($name));
            }
            return new \Ejz\DatabasePool($objects);
        case \Ejz\Bitmap::class:
            [$name] = $args;
            $name = strtoupper($name);
            $host = getenv("BITMAP_{$name}_HOST") ?: getenv('BITMAP_HOST');
            $port = getenv("BITMAP_{$name}_PORT") ?: getenv('BITMAP_PORT');
            $auth = getenv("BITMAP_{$name}_AUTH") ?: getenv('BITMAP_AUTH');
            $auth = $auth ? $auth . '@' : '';
            $dsn = "http://{$auth}{$host}:{$port}";
            $config = [];
            if (getenv('DEBUG')) {
                $config['logger'] = function ($query, $result) {
                    echo ($l = str_repeat('-', 10) . date(' H:i:s ') . str_repeat('-', 10)), "\n";
                    echo trim($query), "\n";
                    echo json_encode($result, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE), "\n";
                    echo str_repeat('-', strlen($l)), "\n";
                };
            }
            return new \Ejz\Bitmap($name, $dsn, $config);
        case \Ejz\BitmapPool::class:
            $pool = getenv('BITMAP_POOL');
            $objects = [];
            foreach (explode(',', $pool) as $name) {
                $objects[] = get(\Ejz\Bitmap::class, strtolower($name));
            }
            return new \Ejz\BitmapPool($objects);
        case \Ejz\RepositoryPool::class:
            [$configs] = $args;
            $objects = [];
            foreach ($configs as $name => $config) {
                $objects[] = get(\Ejz\Repository::class, $name, $config);
            }
            return new \Ejz\RepositoryPool($objects);
        case \Ejz\Repository::class:
            [$name, $config] = $args;
            return new \Ejz\Repository(
                $name,
                $config,
                get(\Ejz\DatabasePool::class),
                get(\Ejz\BitmapPool::class),
                get(\Ejz\RedisCache::class)
            );
    }
}
