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
        $p->db(0)->drop('t');
        \Amp\Promise\wait(\Amp\Promise\some($p->execAsync('create table t()')));
        $tables = \Amp\Promise\wait(\Amp\Promise\all($p->tablesAsync()));
        $this->assertTrue($tables[0][0] === 't');
        $this->assertTrue($tables[1][0] === 't');
        $this->assertTrue($tables[2][0] === 't');
    }
}
