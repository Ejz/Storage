<?php

namespace Tests;

class TestCaseDatabasePostgres extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_postgres_common_1()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (tt TEXT)');
        $n = $db->exec('INSERT INTO t (tt) VALUES (\'bar\')');
        $this->assertTrue($n === 1);
        $n = $db->exec('DELETE FROM t WHERE tt = \'foo\'');
        $this->assertTrue($n === 0);
        $n = $db->exec('DELETE FROM t WHERE tt = \'bar\'');
        $this->assertTrue($n === 1);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_common_2()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $n = $db->exec('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg1', 'arg2');
        $this->assertTrue($n === 1);
        $n = $db->exec('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg3', 'arg4');
        $this->assertTrue($n === 1);
        $n = $db->exec('INSERT INTO t (t1, t2) VALUES ($1, $2)', 'arg5', 'arg6');
        $this->assertTrue($n === 1);
        $all = $db->all('SELECT * FROM t ORDER BY t1 ASC');
        $this->assertTrue($all[0]['t1'] === 'arg1');
        $this->assertTrue($all[1]['t1'] === 'arg3');
        $one = $db->one('SELECT * FROM t ORDER BY t1 ASC');
        $this->assertTrue($one['t1'] === 'arg1');
        $val = $db->val('SELECT t1 FROM t ORDER BY t1 ASC');
        $this->assertTrue($val === 'arg1');
        $val = $db->val('SELECT t1 FROM t WHERE t1 = ? ORDER BY t1 ASC', time());
        $this->assertTrue($val === null);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_one_async()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($db) {
            $one = yield $db->oneAsync('SELECT * FROM t');
            $this->assertTrue(isset($one['t1']));
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_val_async()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        \Amp\Loop::run(function () use ($db) {
            $t2 = yield $db->valAsync('SELECT t2 FROM t');
            $this->assertTrue($t2 == 2);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_col_async()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (t1 INT, t2 TEXT)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($db) {
            $col = yield $db->colAsync('SELECT t1, t2 FROM t');
            $this->assertTrue($col === [1, 3]);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_map_async()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($db) {
            $map = yield $db->mapAsync('SELECT t2, t1 FROM t');
            $this->assertTrue($map[2] === '1');
            $this->assertTrue($map[4] === '3');
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_dict_async()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $db->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($db) {
            $dict = yield $db->dictAsync('SELECT t1, t2 FROM t');
            $this->assertTrue($dict[1]['t2'] === '2');
            $this->assertTrue($dict[3]['t2'] === '4');
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_tables()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t1 (t1 TEXT, t2 TEXT)');
        $db->exec('CREATE TABLE t2 (t1 TEXT, t2 TEXT)');
        $this->assertTrue(in_array('t1', $db->tables()));
        $this->assertTrue(in_array('t2', $db->tables()));
        $this->assertTrue(!in_array('ttt' . mt_rand(), $db->tables()));
        \Amp\Loop::run(function () use ($db) {
            $tables = yield $db->tablesAsync();
            $this->assertTrue(in_array('t1', $tables));
            $this->assertTrue(in_array('t2', $tables));
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_fields()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t1 (t1 TEXT, ti INT NOT NULL, pk1 INT NOT NULL, pk2 INT NOT NULL)');
        $db->exec('CREATE TABLE t2 (t2 TEXT)');
        $db->exec('CREATE TABLE t3 ()');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)');
        $this->assertEquals(null, $db->fields('unknown'));
        $this->assertEquals(null, $db->fields(''));
        $fields = $db->fields('t1');
        $this->assertEquals(['t1', 'ti', 'pk1', 'pk2'], $fields);
        $fields = $db->fields('t2');
        $this->assertEquals(['t2'], $fields);
        $fields = $db->fields('t3');
        $this->assertEquals([], $fields);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_pk()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t0 (t0 TEXT)');
        $db->exec('CREATE TABLE t1 (t1 TEXT, pk1 INT NOT NULL)');
        $db->exec('CREATE TABLE t2 (t1 TEXT, pk1 INT NOT NULL, pk2 INT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)');
        $db->exec('ALTER TABLE ONLY t2 ADD CONSTRAINT pk12_pkey PRIMARY KEY (pk1, pk2)');
        $this->assertTrue($db->pk('') === null);
        $this->assertTrue($db->pk('tt' . mt_rand()) === null);
        $this->assertTrue($db->pk('t0') === []);
        $this->assertTrue($db->pk('t1') === ['pk1']);
        $this->assertTrue($db->pk('t2') === ['pk1', 'pk2']);
        \Amp\Loop::run(function () use ($db) {
            $this->assertTrue((yield $db->pkAsync('t1')) === ['pk1']);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_min_max()
    {
        $db = $this->pool->random();
        //
        $db->exec('CREATE TABLE t1 (pk INT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk)');
        $this->assertEquals([null], $db->min('t1'));
        $this->assertEquals([null], $db->max('t1'));
        $is = range(10, 90);
        shuffle($is);
        foreach ($is as $i) {
            $db->exec('INSERT INTO t1 (pk) VALUES (?)', $i);
        }
        $this->assertTrue(is_int($db->min('t1')[0]));
        $this->assertTrue(is_int($db->max('t1')[0]));
        $this->assertEquals([10], $db->min('t1'));
        $this->assertEquals([90], $db->max('t1'));
        //
        $db->exec('CREATE TABLE t2 (pk1 TEXT NOT NULL, pk2 TEXT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t2 ADD CONSTRAINT pk12_pkey PRIMARY KEY (pk1, pk2)');
        $this->assertEquals([null, null], $db->min('t2'));
        $this->assertEquals([null, null], $db->max('t2'));
        $db->exec('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '2', 'b');
        $db->exec('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '3', 'c');
        $db->exec('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '1', 'a');
        $this->assertTrue(!is_int($db->min('t2')[0]));
        $this->assertTrue(!is_int($db->max('t2')[0]));
        $this->assertEquals(['1', 'a'], $db->min('t2'));
        $this->assertEquals(['3', 'c'], $db->max('t2'));
        //
        $db->exec('CREATE TABLE t3 (pk TEXT NOT NULL)');
        $this->assertEquals([], $db->min('t3'));
        //
        $this->assertEquals(null, $db->min('t4'));
    }
}
