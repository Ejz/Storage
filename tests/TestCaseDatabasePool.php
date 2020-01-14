<?php

namespace Tests;

use function Amp\Promise\wait;

class TestCaseDatabasePool extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_pool_common_1()
    {
        $pool = $this->pool;
        wait(\Amp\Promise\all($pool->exec('create table t()')));
        wait(\Amp\Promise\any($pool->exec('create table t()')));
        $names = $pool->names();
        $this->assertTrue($pool->db(mt_rand()) === null);
        $pool->db($names[0])->drop('t');
        $tables = wait(\Amp\Promise\all($pool->tables()));
        $this->assertTrue(!in_array('t', $tables[$names[0]]));
        $this->assertTrue(in_array('t', $tables[$names[1]]));
        $this->assertTrue(in_array('t', $tables[$names[2]]));
        wait(\Amp\Promise\some($pool->exec('create table t()')));
        $tables = wait(\Amp\Promise\all($pool->tables()));
        $this->assertTrue(in_array('t', $tables[$names[0]]));
        $this->assertTrue(in_array('t', $tables[$names[1]]));
        $this->assertTrue(in_array('t', $tables[$names[2]]));
    }
}
