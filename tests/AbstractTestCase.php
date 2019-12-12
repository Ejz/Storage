<?php

namespace Tests;

use ReflectionClass;
use PHPUnit\Framework\TestCase;
use Ejz\DatabasePostgres;
use Ejz\DatabasePool;
use Ejz\Storage;
use Ejz\RedisCache;
use Ejz\RedisClient;
use Ejz\TableDefinition;
use Ejz\Bitmap;

abstract class AbstractTestCase extends TestCase
{
    /** DatabasePostgres */
    protected $database;

    /** DatabasePool */
    protected $pool;

    /** RedisCache */
    protected $cache;

    /**
     * @return void
     */
    protected function setUp(): void
    {
        $this->database = $this->getDatabasePostgres('db0');
        $this->pool = $this->getDatabasePool();
        $this->cache = $this->getRedisCache('db0');
        $this->bitmap = $this->getBitmap('db0');
    }

    /**
     * @return void
     */
    protected function tearDown(): void
    {
        $this->pool->forEach(function ($db) {
            foreach ($db->tables() as $table) {
                $db->drop($table);
            }
        });
    }

    /**
     * @param string $name
     *
     * @return DatabasePostgres
     */
    protected function getDatabasePostgres(string $name): DatabasePostgres
    {
        $_name = strtoupper($name);
        $dsn = sprintf(
            'host=%s port=%s user=%s password=%s db=%s',
            getenv("POSTGRES_{$_name}_HOST"),
            getenv("POSTGRES_{$_name}_PORT"),
            getenv("POSTGRES_{$_name}_USER"),
            getenv("POSTGRES_{$_name}_PASSWORD"),
            getenv("POSTGRES_{$_name}_DB")
        );
        return new DatabasePostgres($name, $dsn);
    }

    /**
     * @return DatabasePool
     */
    protected function getDatabasePool(): DatabasePool
    {
        return new DatabasePool([
            $this->getDatabasePostgres('db0'),
            $this->getDatabasePostgres('db1'),
            $this->getDatabasePostgres('db2'),
        ]);
    }

    /**
     * @param string $name
     *
     * @return RedisCache
     */
    protected function getRedisCache(string $name): RedisCache
    {
        $_name = strtoupper($name);
        $host = getenv("REDIS_{$_name}_HOST");
        $port = getenv("REDIS_{$_name}_PORT");
        $persistent = false;
        return new RedisCache(new RedisClient(compact('persistent', 'host', 'port')));
    }

    /**
     * @param string $name
     *
     * @return Bitmap
     */
    protected function getBitmap(string $name): Bitmap
    {
        $_name = strtoupper($name);
        $host = getenv("BITMAP_{$_name}_HOST");
        $port = getenv("BITMAP_{$_name}_PORT");
        $persistent = false;
        return new Bitmap(new RedisClient(compact('persistent', 'host', 'port')));
    }

    /**
     * @param array $tables
     *
     * @return Storage
     */
    protected function getStorage(array $tables): Storage
    {
        return new Storage($this->pool, $this->cache, $this->bitmap, $tables);
    }

    /**
     * @param object &$object
     * @param string $method
     * @param array  ...$arguments
     *
     * @return mixed
     */
    protected function call(object &$object, string $method, ...$arguments)
    {
        $reflection = new ReflectionClass(get_class($object));
        $method = $reflection->getMethod($method);
        $method->setAccessible(true);
        return $method->invokeArgs($object, $arguments);
    }
}
