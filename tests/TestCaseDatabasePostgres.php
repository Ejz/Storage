<?php

namespace Tests;

class TestCaseDatabasePostgres extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_postgres_common_1()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (tt TEXT)');
        $n = $d->exec('INSERT INTO t (tt) VALUES (\'bar\')');
        $this->assertTrue($n === 1);
        $n = $d->exec('DELETE FROM t WHERE tt = \'foo\'');
        $this->assertTrue($n === 0);
        $n = $d->exec('DELETE FROM t WHERE tt = \'bar\'');
        $this->assertTrue($n === 1);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_common_2()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $n = $d->exec('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg1', 'arg2');
        $this->assertTrue($n === 1);
        $n = $d->exec('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg3', 'arg4');
        $this->assertTrue($n === 1);
        $n = $d->exec('INSERT INTO t (t1, t2) VALUES ($1, $2)', 'arg5', 'arg6');
        $this->assertTrue($n === 1);
        $all = $d->all('SELECT * FROM t ORDER BY t1 ASC');
        $this->assertTrue($all[0]['t1'] === 'arg1');
        $this->assertTrue($all[1]['t1'] === 'arg3');
        $one = $d->one('SELECT * FROM t ORDER BY t1 ASC');
        $this->assertTrue($one['t1'] === 'arg1');
        $val = $d->val('SELECT t1 FROM t ORDER BY t1 ASC');
        $this->assertTrue($val === 'arg1');
        $val = $d->val('SELECT t1 FROM t WHERE t1 = ? ORDER BY t1 ASC', time());
        $this->assertTrue($val === null);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_one_async()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($d) {
            $one = yield $d->oneAsync('SELECT * FROM t');
            $this->assertTrue(isset($one['t1']));
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_val_async()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        \Amp\Loop::run(function () use ($d) {
            $t2 = yield $d->valAsync('SELECT t2 FROM t');
            $this->assertTrue($t2 == 2);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_col_async()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (t1 INT, t2 TEXT)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($d) {
            $col = yield $d->colAsync('SELECT t1, t2 FROM t');
            $this->assertTrue($col === [1, 3]);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_map_async()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($d) {
            $map = yield $d->mapAsync('SELECT t2, t1 FROM t');
            $this->assertTrue($map[2] === '1');
            $this->assertTrue($map[4] === '3');
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_dict_async()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (1, 2)');
        $d->exec('INSERT INTO t (t1, t2) VALUES (3, 4)');
        \Amp\Loop::run(function () use ($d) {
            $dict = yield $d->dictAsync('SELECT t1, t2 FROM t');
            $this->assertTrue($dict[1]['t2'] === '2');
            $this->assertTrue($dict[3]['t2'] === '4');
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_tables()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t1 (t1 TEXT, t2 TEXT)');
        $d->exec('CREATE TABLE t2 (t1 TEXT, t2 TEXT)');
        $this->assertTrue(in_array('t1', $d->tables()));
        $this->assertTrue(in_array('t2', $d->tables()));
        $this->assertTrue(!in_array('ttt' . mt_rand(), $d->tables()));
        \Amp\Loop::run(function () use ($d) {
            $tables = yield $d->tablesAsync();
            $this->assertTrue(in_array('t1', $tables));
            $this->assertTrue(in_array('t2', $tables));
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_fields()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t1 (t1 TEXT, ti INT NOT NULL, pk1 INT NOT NULL, pk2 INT NOT NULL)');
        $d->exec('CREATE TABLE t2 (t2 TEXT)');
        $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)');
        $this->assertEquals([], $d->fields('unknown'));
        $fields = $d->fields('t1');
        $this->assertTrue(isset($fields['t1']));
        $this->assertTrue($fields['t1']['type'] === 'text');
        $this->assertTrue($fields['t1']['is_null'] === true);
        $this->assertTrue($fields['ti']['type'] === 'integer');
        $this->assertTrue($fields['ti']['is_null'] === false);
        $this->assertTrue($fields['pk1']['is_primary'] === true);
        $fields = $d->fields('t2');
        $this->assertTrue($fields['t2']['type'] === 'text');
    }

    /**
     * @test
     */
    public function test_case_database_postgres_pks()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t1 (t1 TEXT, pk1 INT NOT NULL)');
        $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)');
        $this->assertTrue($d->pk('t1') === 'pk1');
        \Amp\Loop::run(function () use ($d) {
            $pk = yield $d->pkAsync('t1');
            $this->assertTrue($pk === 'pk1');
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_min_max_1()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t1 (pk INT NOT NULL)');
        $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk)');
        $is = range(10, 90);
        shuffle($is);
        foreach ($is as $i) {
            $d->exec('INSERT INTO t1 (pk) VALUES (?)', $i);
        }
        $this->assertEquals(0, $d->min('unknown'));
        $this->assertEquals(10, $d->min('t1'));
        $this->assertEquals(90, $d->max('t1'));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterator_1()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
        $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
        $is = [];
        foreach (range(40, 50) as $_id) {
            $is[] = $_id;
            $d->exec('INSERT INTO t1 (pk1) VALUES (?)', $_id);
        }
        \Amp\Loop::run(function () use ($d, &$is) {
            $iterator = $d->iterateAsync('t1');
            while (yield $iterator->advance()) {
                [$key, $row] = $iterator->getCurrent();
                $this->assertEquals(array_values($row)[0], $key);
                $k = array_search($key, $is);
                $this->assertTrue($k !== false);
                unset($is[$k]);
            }
        });
        $this->assertTrue($is === []);
        foreach ($d->iterate('t1') as [$key, $row]) {
            $this->assertEquals(array_values($row)[0], $key);
        }
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterator_2()
    {
        $d = $this->database;
        $d->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
        $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
        $is = [];
        foreach (range(40, 50) as $_2) {
            $is[] = $_2;
            $d->exec('INSERT INTO t1 (pk1) VALUES (?)', $_2);
        }
        $config = ['rand_iterator_intervals' => 10];
        $generator = $d->iterate('t1', ['rand' => true, 'config' => $config]);
        foreach ($generator as [$key, $row]) {
            $k = array_search($key, $is);
            $this->assertTrue($k !== false, $key);
            unset($is[$k]);
        }
        $this->assertTrue($is === []);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_rand_intervals()
    {
        $d = $this->database;
        //
        $intervals = $this->call($d, 'getIntervalsForRandIterator', 1, 3, 3);
        $this->assertEquals([[1, 1], [2, 2], [3, 3]], $intervals);
        $intervals = $this->call($d, 'getIntervalsForRandIterator', 1, 3, 30);
        $this->assertEquals([1, 1], $intervals[0]);
        $this->assertEquals([3, 3], $intervals[count($intervals) - 1]);
        $intervals = $this->call($d, 'getIntervalsForRandIterator', 1, 30, 10);
        $this->assertEquals([1, 3], $intervals[0]);
        $this->assertEquals([28, 30], $intervals[count($intervals) - 1]);
    }
}