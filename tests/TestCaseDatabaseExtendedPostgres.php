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
}
// /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterator_1()
    // {
    //     $d = $this->database;
    //     $d->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
    //     $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
    //     $is = [];
    //     foreach (range(40, 50) as $_id) {
    //         $is[] = $_id;
    //         $d->exec('INSERT INTO t1 (pk1) VALUES (?)', $_id);
    //     }
    //     \Amp\Loop::run(function () use ($d, &$is) {
    //         $iterator = $d->iterate('t1');
    //         while (yield $iterator->advance()) {
    //             [$key, $row] = $iterator->getCurrent();
    //             $this->assertEquals(array_values($row)[0], $key);
    //             $k = array_search($key, $is);
    //             $this->assertTrue($k !== false);
    //             unset($is[$k]);
    //         }
    //     });
    //     $this->assertTrue($is === []);
    //     $iterator = $d->iterate('t1');
    //     while (
    //         [$key, $row] = \Amp\Promise\wait(\Amp\call(function ($iterator) {
    //             if (yield $iterator->advance()) {
    //                 return $iterator->getCurrent();
    //             }
    //         }, $iterator))
    //     ) {
    //         $this->assertEquals(array_values($row)[0], $key);
    //     }
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterator_2()
    // {
    //     $d = $this->database;
    //     $d->exec('CREATE TABLE t1 (pk1 INT NOT NULL)');
    //     $d->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)');
    //     $is = [];
    //     foreach (range(40, 50) as $_2) {
    //         $is[] = $_2;
    //         $d->exec('INSERT INTO t1 (pk1) VALUES (?)', $_2);
    //     }
    //     $config = ['rand_iterator_intervals' => 10];
    //     $iterator = $d->iterate('t1', ['rand' => true, 'config' => $config]);
    //     while (
    //         [$key, $row] = \Amp\Promise\wait(\Amp\call(function ($iterator) {
    //             if (yield $iterator->advance()) {
    //                 return $iterator->getCurrent();
    //             }
    //         }, $iterator))
    //     ) {
    //         $k = array_search($key, $is);
    //         $this->assertTrue($k !== false, $key);
    //         unset($is[$k]);
    //     }
    //     $this->assertTrue($is === []);
    // }