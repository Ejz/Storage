<?php

namespace Tests;

use ReflectionClass;
use PHPUnit\Framework\TestCase;
use Ejz\DatabasePool;
use Ejz\RedisCache;
use Ejz\Bitmap;

use function Amp\Promise\wait;

abstract class AbstractTestCase extends TestCase
{
    /** RedisCache */
    protected $cache;

    /** DatabasePool */
    protected $databasePool;

    /** BitmapPool */
    protected $bitmapPool;

    /** RepositoryPool */
    protected $repositoryPool;

    /**
     * @return void
     */
    protected function setUp(): void
    {
        $this->databasePool = \Container\get(\Ejz\DatabasePool::class);
        $tables = $this->databasePool->tablesSync();
        $tables = array_merge(...array_values($tables));
        $tables = array_unique($tables);
        foreach ($tables as $table) {
            $this->databasePool->dropSync($table);
        }
        $this->bitmapPool = \Container\get(\Ejz\BitmapPool::class);
        $indexes = $this->bitmapPool->indexesSync();
        $indexes = array_merge(...array_values($indexes));
        $indexes = array_unique($indexes);
        foreach ($indexes as $index) {
            $this->bitmapPool->dropSync($index);
        }
        $this->cache = \Container\get(\Ejz\RedisCache::class);
        $this->cache->getClient()->FLUSHDB();
    }

    /**
     * @return void
     */
    protected function tearDown(): void
    {
        $this->databasePool->close();
    }

    /**
     * @param object &$object
     * @param string $method
     *
     * @return mixed
     */
    protected function getPrivateProperty(object $object, string $property)
    {
        $reflection = new ReflectionClass(get_class($object));
        $property = $reflection->getProperty($property);
        $property->setAccessible(true);
        return $property->getValue($object);
    }

    /**
     * @param object &$object
     * @param string $method
     * @param array  ...$arguments
     *
     * @return mixed
     */
    protected function callPrivateMethod(object $object, string $method, ...$arguments)
    {
        $reflection = new ReflectionClass(get_class($object));
        $method = $reflection->getMethod($method);
        $method->setAccessible(true);
        return $method->invokeArgs($object, $arguments);
    }
}
