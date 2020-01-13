<?php

namespace Tests;

class TestCaseDatabaseExtendedPostgres extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_extended_postgres_rand_intervals()
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

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_1()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
        $ids = [];
        foreach (range(40, 50) as $id) {
            $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
            $ids[] = $id;
        }
        $_ids = $ids;
        $asc = (bool) mt_rand(0, 1);
        $params = ['asc' => $asc, 'config' => ['iterator_chunk_size' => mt_rand(1, 4)]];
        foreach ($db->iterate('t1', $params) as $key => $value) {
            $k = array_search($key, $_ids);
            $this->assertTrue(count($value) === 1);
            $this->assertTrue($k !== false);
            unset($_ids[$k]);
        }
        $this->assertTrue($_ids === []);
        $this->assertEquals(11, count(iterator_to_array($db->iterate('t1'))));
        $this->assertEquals(11, count(iterator_to_array($db->iterate('t1', $params))));
        $_params = ['max' => 49] + $params;
        $this->assertEquals(10, count(iterator_to_array($db->iterate('t1', $_params))));
        $_params = ['min' => 42] + $params;
        $this->assertEquals(9, count(iterator_to_array($db->iterate('t1', $_params))));
        $_params = ['min' => 42, 'max' => 47] + $params;
        $this->assertEquals(6, count(iterator_to_array($db->iterate('t1', $_params))));
        $_params = ['min' => 42, 'max' => 47, 'limit' => 3] + $params;
        $this->assertEquals(3, count(iterator_to_array($db->iterate('t1', $_params))));
        $_params = ['fields' => []] + $params;
        $_ = iterator_to_array($db->iterate('t1', $_params));
        $this->assertEquals([], current($_));
        $this->assertEquals($asc ? 40 : 50, key($_));
        $_params = ['asc' => !$asc] + $params;
        $_ = iterator_to_array($db->iterate('t1', $_params));
        $this->assertEquals($asc ? 50 : 40, key($_));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_2()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
        $ids = [];
        foreach (range(10, 100) as $id) {
            $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
            $ids[] = $id;
        }
        $params = ['config' => [
            'iterator_chunk_size' => mt_rand(1, 100),
            'rand_iterator_intervals' => mt_rand(1, 100),
        ]];
        $ids = array_keys(iterator_to_array($db->iterate('t1', ['rand' => true])));
        $this->assertFalse($ids[0] === 10 && $ids[count($ids) - 1] === 100);
        $this->assertFalse($ids[0] === 100 && $ids[count($ids) - 1] === 10);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_3()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t1 (pk1 TEXT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
        $ids = [];
        foreach (range('a', 'z') as $id) {
            $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
            $ids[] = $id;
        }
        $params = ['config' => ['iterator_chunk_size' => mt_rand(2, 4)]];
        $ids = array_keys(iterator_to_array($db->iterate('t1', $params)));
        $this->assertTrue($ids[0] === 'a' && $ids[count($ids) - 1] === 'z');
    }

    /**
     * @test
     */
    public function test_case_database_postgres_get()
    {
        $db = $this->pool->random();
        $db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
        $db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
        $ids = [];
        foreach (range(1, 1000) as $id) {
            $db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id);
            $ids[] = $id;
        }
        $this->assertTrue(iterator_to_array($db->get('t1', [1E6])) === []);
        $this->assertTrue(iterator_to_array($db->get('t2', [1E6])) === []);
        $_ = iterator_to_array($db->get('t1', [1, 2], ['fields' => []]));
        $this->assertTrue($_ === [1 => [], 2 => []]);
        // var_dump();
        // $ids = array_keys(iterator_to_array($db->iterate('t1', $params)));
        // $this->assertTrue($ids[0] === 'a' && $ids[count($ids) - 1] === 'z');
    }
}
