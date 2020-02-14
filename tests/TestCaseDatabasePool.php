<?php

namespace Tests;

use function Amp\Promise\wait;

class TestCaseDatabasePool extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_pool()
    {
        $pool = $this->databasePool;
        $pool->execSync('create table t()');
        $res = $pool->each(function ($db) {
            return $db->tableExistsSync('t');
        });
        $this->assertEquals(array_fill_keys($pool->names(), true), $res);
    }
}
