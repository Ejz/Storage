<?php

namespace Tests;

use ReflectionClass;
use PHPUnit\Framework\TestCase;
use Ejz\DatabasePostgres;

abstract class AbstractTestCase extends TestCase
{
    /**
     * @param int $n (optional)
     *
     * @return DatabasePostgres
     */
    protected function getDatabasePostgres(int $n = 0): DatabasePostgres
    {
        $dsn = sprintf(
            'host=%s port=%s user=%s password=%s db=%s',
            getenv("POSTGRES{$n}_HOST"),
            getenv("POSTGRES{$n}_PORT"),
            getenv("POSTGRES{$n}_USER"),
            getenv("POSTGRES{$n}_PASSWORD"),
            getenv("POSTGRES{$n}_DB")
        );
        return new DatabasePostgres($dsn);
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
