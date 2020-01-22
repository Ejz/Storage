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
    }

    /**
     * @return void
     */
    protected function tearDown(): void
    {
        $this->pool->each(function ($db) {
            foreach (wait($db->tables()) as $table) {
                $this->bitmap->DROP($table);
                wait($db->drop($table));
            }
            $db->close();
        });
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
