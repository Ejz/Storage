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
    /** Pool */
    protected $databasePool;

    /** Pool */
    protected $bitmapPool;

    /** RedisCache */
    protected $cache;

    /**
     * @return void
     */
    protected function setUp(): void
    {
        $this->databasePool = \Container\getDatabasePool();
        $this->bitmapPool = \Container\getBitmapPool();
        $this->cache = \Container\getCache();
        $names = $this->databasePool->names();
        foreach ($names as $name) {
            $db = $this->databasePool->instance($name);
            $tables = $db->tablesSync();
            foreach ($tables as $table) {
                $db->dropSync($table);
            }
        }
        $names = []; // $this->bitmapPool->names();
        foreach ($names as $name) {
            $bitmap = $this->bitmapPool->instance($name);
            $list = $bitmap->LIST();
            foreach ($list as $index) {
                $bitmap->DROP($index);
            }
        }
        $this->cache->clear();
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
