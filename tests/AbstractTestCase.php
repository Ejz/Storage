<?php

namespace Tests;

use ReflectionClass;
use PHPUnit\Framework\TestCase;
use Ejz\DatabasePostgres;
use Ejz\DatabasePool;
use Ejz\Storage;
use Ejz\TableDefinition;

abstract class AbstractTestCase extends TestCase
{
    /** DatabasePostgres */
    protected $database;

    /**
     * @return void
     */
    protected function setUp(): void
    {
        $this->database = $this->getDatabasePostgres();
    }

    /**
     * @return void
     */
    protected function tearDown(): void
    {
        $tables = $this->database->tables();
        foreach ($tables as $table) {
            $this->database->drop($table);
        }
    }

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
     * @return DatabasePool
     */
    protected function getDatabasePool(): DatabasePool
    {
        return new DatabasePool([
            $this->getDatabasePostgres(0),
            $this->getDatabasePostgres(1),
            $this->getDatabasePostgres(2),
        ]);
    }

    /**
     * @param array $tables
     *
     * @return Storage
     */
    protected function getStorage(array $tables): Storage
    {
        $pool = $this->getDatabasePool();
        return new Storage($pool, $tables);
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
