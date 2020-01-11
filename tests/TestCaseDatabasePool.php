<?php

namespace Tests;

class TestCaseDatabasePool extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_pool_common_1()
    {
        $pool = $this->pool;
        \Amp\Promise\wait(\Amp\Promise\all($pool->execAsync('create table t()')));
        \Amp\Promise\wait(\Amp\Promise\any($pool->execAsync('create table t()')));
        $names = $pool->names();
        $this->assertTrue($pool->db(mt_rand()) === null);
        $pool->db($names[0])->drop('t');
        $tables = \Amp\Promise\wait(\Amp\Promise\all($pool->tablesAsync()));
        $this->assertTrue(!in_array('t', $tables[$names[0]]));
        $this->assertTrue(in_array('t', $tables[$names[1]]));
        $this->assertTrue(in_array('t', $tables[$names[2]]));
        \Amp\Promise\wait(\Amp\Promise\some($pool->execAsync('create table t()')));
        $tables = \Amp\Promise\wait(\Amp\Promise\all($pool->tablesAsync()));
        $this->assertTrue(in_array('t', $tables[$names[0]]));
        $this->assertTrue(in_array('t', $tables[$names[1]]));
        $this->assertTrue(in_array('t', $tables[$names[2]]));
    }
}
