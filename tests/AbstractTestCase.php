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
    /** DatabasePool */
    protected $pool;

    /** RedisCache */
    protected $cache;

    /** Bitmap */
    protected $bitmap;

    /**
     * @return void
     */
    protected function setUp(): void
    {
        $this->pool = \Container\getDatabasePool();
        $this->cache = \Container\getCache();
        $this->bitmap = \Container\getBitmap();
        $names = $this->pool->names();
        foreach ($names as $name) {
            $db = $this->pool->db($name);
            $tables = $db->tablesSync();
            foreach ($tables as $table) {
                $db->dropSync($table);
            }
        }
        $list = $this->bitmap->LIST();
        foreach ($list as $index) {
            $list = $this->bitmap->DROP($index);
        }
        $this->cache->clear();
    }

    /**
     * @return void
     */
    protected function tearDown(): void
    {
        $this->pool->close();
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
