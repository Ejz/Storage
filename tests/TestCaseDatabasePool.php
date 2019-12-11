<?php

namespace Tests;

class TestCaseDatabasePool extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_pool_1()
    {
        $p = $this->pool;
        \Amp\Promise\wait(\Amp\Promise\all($p->execAsync('create table t()')));
        \Amp\Promise\wait(\Amp\Promise\any($p->execAsync('create table t()')));
        $names = $p->names();
        $p->db($names[0])->drop('t');
        $tables = \Amp\Promise\wait(\Amp\Promise\all($p->tablesAsync()));
        $this->assertTrue(!in_array('t', $tables[$names[0]]));
        $this->assertTrue(in_array('t', $tables[$names[1]]));
        $this->assertTrue(in_array('t', $tables[$names[2]]));
        \Amp\Promise\wait(\Amp\Promise\some($p->execAsync('create table t()')));
        $tables = \Amp\Promise\wait(\Amp\Promise\all($p->tablesAsync()));
        $this->assertTrue(in_array('t', $tables[$names[0]]));
        $this->assertTrue(in_array('t', $tables[$names[1]]));
        $this->assertTrue(in_array('t', $tables[$names[2]]));
    }
}
