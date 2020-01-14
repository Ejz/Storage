<?php

namespace Tests;

use function Amp\Promise\wait;

class TestCaseDatabasePostgres extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_postgres_common_1()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t (tt TEXT)'));
        $n = wait($db->exec('INSERT INTO t (tt) VALUES (\'bar\')'));
        $this->assertTrue($n === 1);
        $n = wait($db->exec('DELETE FROM t WHERE tt = \'foo\''));
        $this->assertTrue($n === 0);
        $n = wait($db->exec('DELETE FROM t WHERE tt = \'bar\''));
        $this->assertTrue($n === 1);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_common_2()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)'));
        $n = wait($db->exec('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg1', 'arg2'));
        $this->assertTrue($n === 1);
        $n = wait($db->exec('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg3', 'arg4'));
        $this->assertTrue($n === 1);
        $n = wait($db->exec('INSERT INTO t (t1, t2) VALUES ($1, $2)', 'arg5', 'arg6'));
        $this->assertTrue($n === 1);
        $all = wait($db->all('SELECT * FROM t ORDER BY t1 ASC'));
        $this->assertTrue($all[0]['t1'] === 'arg1');
        $this->assertTrue($all[1]['t1'] === 'arg3');
        $one = wait($db->one('SELECT * FROM t ORDER BY t1 ASC'));
        $this->assertTrue($one['t1'] === 'arg1');
        $val = wait($db->val('SELECT t1 FROM t ORDER BY t1 ASC'));
        $this->assertTrue($val === 'arg1');
        $val = wait($db->val('SELECT t1 FROM t WHERE t1 = ? ORDER BY t1 ASC', time()));
        $this->assertTrue($val === null);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_one()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)'));
        wait($db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)'));
        wait($db->exec('INSERT INTO t (t1, t2) VALUES (3, 4)'));
        \Amp\Loop::run(function () use ($db) {
            $one = yield $db->one('SELECT * FROM t');
            $this->assertTrue(isset($one['t1']));
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_val()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t (t1 TEXT, t2 TEXT)'));
        wait($db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)'));
        \Amp\Loop::run(function () use ($db) {
            $t2 = yield $db->val('SELECT t2 FROM t');
            $this->assertTrue($t2 == 2);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_col()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t (t1 INT, t2 TEXT)'));
        wait($db->exec('INSERT INTO t (t1, t2) VALUES (1, 2)'));
        wait($db->exec('INSERT INTO t (t1, t2) VALUES (3, 4)'));
        \Amp\Loop::run(function () use ($db) {
            $col = yield $db->col('SELECT t1, t2 FROM t');
            $this->assertTrue($col === [1, 3]);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_tables()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t1 (t1 TEXT, t2 TEXT)'));
        wait($db->exec('CREATE TABLE t2 (t1 TEXT, t2 TEXT)'));
        $this->assertTrue(in_array('t1', wait($db->tables())));
        $this->assertTrue(in_array('t2', wait($db->tables())));
        $this->assertTrue(!in_array('ttt' . mt_rand(), wait($db->tables())));
        \Amp\Loop::run(function () use ($db) {
            $tables = yield $db->tables();
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
        wait($db->exec('CREATE TABLE t1 (t1 TEXT, ti INT NOT NULL, pk1 INT NOT NULL, pk2 INT NOT NULL)'));
        wait($db->exec('CREATE TABLE t2 (t2 TEXT)'));
        wait($db->exec('CREATE TABLE t3 ()'));
        wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)'));
        $this->assertEquals(null, wait($db->fields('unknown')));
        $this->assertEquals(null, wait($db->fields('')));
        $fields = wait($db->fields('t1'));
        $this->assertEquals(['t1', 'ti', 'pk1', 'pk2'], $fields);
        $fields = wait($db->fields('t2'));
        $this->assertEquals(['t2'], $fields);
        $fields = wait($db->fields('t3'));
        $this->assertEquals([], $fields);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_pk()
    {
        $db = $this->pool->random();
        wait($db->exec('CREATE TABLE t0 (t0 TEXT)'));
        wait($db->exec('CREATE TABLE t1 (t1 TEXT, pk1 INT NOT NULL)'));
        wait($db->exec('CREATE TABLE t2 (t1 TEXT, pk1 INT NOT NULL, pk2 INT NOT NULL)'));
        wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)'));
        wait($db->exec('ALTER TABLE ONLY t2 ADD CONSTRAINT pk12_pkey PRIMARY KEY (pk1, pk2)'));
        $this->assertTrue(wait($db->pk('')) === null);
        $this->assertTrue(wait($db->pk('tt' . mt_rand())) === null);
        $this->assertTrue(wait($db->pk('t0')) === []);
        $this->assertTrue(wait($db->pk('t1')) === ['pk1']);
        $this->assertTrue(wait($db->pk('t2')) === ['pk1', 'pk2']);
        \Amp\Loop::run(function () use ($db) {
            $this->assertTrue((yield $db->pk('t1')) === ['pk1']);
        });
    }

    /**
     * @test
     */
    public function test_case_database_postgres_min_max()
    {
        $db = $this->pool->random();
        //
        wait($db->exec('CREATE TABLE t1 (pk INT NOT NULL)'));
        wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk)'));
        $this->assertEquals([null], wait($db->min('t1')));
        $this->assertEquals([null], wait($db->max('t1')));
        $is = range(10, 90);
        shuffle($is);
        foreach ($is as $i) {
            wait($db->exec('INSERT INTO t1 (pk) VALUES (?)', $i));
        }
        $this->assertTrue(is_int(wait($db->min('t1'))[0]));
        $this->assertTrue(is_int(wait($db->max('t1'))[0]));
        $this->assertEquals([10], wait($db->min('t1')));
        $this->assertEquals([90], wait($db->max('t1')));
        //
        wait($db->exec('CREATE TABLE t2 (pk1 TEXT NOT NULL, pk2 TEXT NOT NULL)'));
        wait($db->exec('ALTER TABLE ONLY t2 ADD CONSTRAINT pk12_pkey PRIMARY KEY (pk1, pk2)'));
        $this->assertEquals([null, null], wait($db->min('t2')));
        $this->assertEquals([null, null], wait($db->max('t2')));
        wait($db->exec('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '2', 'b'));
        wait($db->exec('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '3', 'c'));
        wait($db->exec('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '1', 'a'));
        $this->assertTrue(!is_int(wait($db->min('t2'))[0]));
        $this->assertTrue(!is_int(wait($db->max('t2'))[0]));
        $this->assertEquals(['1', 'a'], wait($db->min('t2')));
        $this->assertEquals(['3', 'c'], wait($db->max('t2')));
        //
        wait($db->exec('CREATE TABLE t3 (pk TEXT NOT NULL)'));
        $this->assertEquals([], wait($db->min('t3')));
        //
        $this->assertEquals(null, wait($db->min('t4')));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_rand_intervals()
    {
        $db = $this->pool->random();
        //
        $intervals = $this->call($db, 'getIntervalsForRandIterator', 1, 3, 3);
        $this->assertEquals([[1, 1], [2, 2], [3, 3]], $intervals);
        $intervals = $this->call($db, 'getIntervalsForRandIterator', 1, 3, 30);
        $this->assertEquals([1, 1], $intervals[0]);
        $this->assertEquals([3, 3], $intervals[count($intervals) - 1]);
        $intervals = $this->call($db, 'getIntervalsForRandIterator', 1, 30, 10);
        $this->assertEquals([1, 3], $intervals[0]);
        $this->assertEquals([28, 30], $intervals[count($intervals) - 1]);
    }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_1()
    // {
    //     $db = $this->pool->random();
    //     $db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
    //     $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
    //     $ids = [];
    //     foreach (range(40, 50) as $id) {
    //         $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
    //         $ids[] = $id;
    //     }
    //     $_ids = $ids;
    //     $asc = (bool) mt_rand(0, 1);
    //     $params = ['asc' => $asc, 'config' => ['iterator_chunk_size' => mt_rand(1, 4)]];
    //     foreach ($db->iterate('t1', $params) as $key => $value) {
    //         $k = array_search($key, $_ids);
    //         $this->assertTrue(count($value) === 1);
    //         $this->assertTrue($k !== false);
    //         unset($_ids[$k]);
    //     }
    //     $this->assertTrue($_ids === []);
    //     $this->assertEquals(11, count(iterator_to_array($db->iterate('t1'))));
    //     $this->assertEquals(11, count(iterator_to_array($db->iterate('t1', $params))));
    //     $_params = ['max' => 49] + $params;
    //     $this->assertEquals(10, count(iterator_to_array($db->iterate('t1', $_params))));
    //     $_params = ['min' => 42] + $params;
    //     $this->assertEquals(9, count(iterator_to_array($db->iterate('t1', $_params))));
    //     $_params = ['min' => 42, 'max' => 47] + $params;
    //     $this->assertEquals(6, count(iterator_to_array($db->iterate('t1', $_params))));
    //     $_params = ['min' => 42, 'max' => 47, 'limit' => 3] + $params;
    //     $this->assertEquals(3, count(iterator_to_array($db->iterate('t1', $_params))));
    //     $_params = ['fields' => []] + $params;
    //     $_ = iterator_to_array($db->iterate('t1', $_params));
    //     $this->assertEquals([], current($_));
    //     $this->assertEquals($asc ? 40 : 50, key($_));
    //     $_params = ['asc' => !$asc] + $params;
    //     $_ = iterator_to_array($db->iterate('t1', $_params));
    //     $this->assertEquals($asc ? 50 : 40, key($_));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_2()
    // {
    //     $db = $this->pool->random();
    //     $db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
    //     $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
    //     $ids = [];
    //     foreach (range(10, 100) as $id) {
    //         $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
    //         $ids[] = $id;
    //     }
    //     $params = ['config' => [
    //         'iterator_chunk_size' => mt_rand(1, 100),
    //         'rand_iterator_intervals' => mt_rand(1, 100),
    //     ]];
    //     $ids = array_keys(iterator_to_array($db->iterate('t1', ['rand' => true])));
    //     $this->assertFalse($ids[0] === 10 && $ids[count($ids) - 1] === 100);
    //     $this->assertFalse($ids[0] === 100 && $ids[count($ids) - 1] === 10);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_3()
    // {
    //     $db = $this->pool->random();
    //     $db->exec('CREATE TABLE t1 (pk1 TEXT NOT NULL)');
    //     $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
    //     $ids = [];
    //     foreach (range('a', 'z') as $id) {
    //         $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
    //         $ids[] = $id;
    //     }
    //     $params = ['config' => ['iterator_chunk_size' => mt_rand(2, 4)]];
    //     $ids = array_keys(iterator_to_array($db->iterate('t1', $params)));
    //     $this->assertTrue($ids[0] === 'a' && $ids[count($ids) - 1] === 'z');
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_get()
    // {
    //     $db = $this->pool->random();
    //     $db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
    //     $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
    //     $ids = [];
    //     foreach (range(1, 1000) as $id) {
    //         $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
    //         $ids[] = $id;
    //     }
    //     $this->assertTrue(iterator_to_array($db->get('t1', [1E6])) === []);
    //     $this->assertTrue(iterator_to_array($db->get('t2', [1E6])) === []);
    //     $_ = iterator_to_array($db->get('t1', [1, 2], ['fields' => []]));
    //     $this->assertTrue($_ === [1 => [], 2 => []]);
    //     // var_dump();
    //     // $ids = array_keys(iterator_to_array($db->iterate('t1', $params)));
    //     // $this->assertTrue($ids[0] === 'a' && $ids[count($ids) - 1] === 'z');
    // }
}
