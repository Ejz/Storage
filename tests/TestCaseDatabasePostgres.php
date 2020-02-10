<?php

namespace Tests;

use Ejz\Field;
use Ejz\WhereCondition;

class TestCaseDatabasePostgres extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_postgres_exec_0()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t (tt TEXT)');
        $n = $db->execSync('INSERT INTO t (tt) VALUES (\'bar\')');
        $this->assertTrue($n === 1);
        $n = $db->execSync('DELETE FROM t WHERE tt = \'foo\'');
        $this->assertTrue($n === 0);
        $n = $db->execSync('DELETE FROM t WHERE tt = \'bar\'');
        $this->assertTrue($n === 1);
        $n = $db->execSync('DELETE FROM t WHERE tt = \'bar\'');
        $this->assertTrue($n === 0);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_exec_1()
    {
        $this->expectException(\Amp\Postgres\QueryExecutionError::class);
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE');
    }

    /**
     * @test
     */
    public function test_case_database_postgres_all_one_col_val()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t (t1 TEXT, t2 TEXT)');
        $db->execSync('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg1', 'arg2');
        $db->execSync('INSERT INTO t (t1, t2) VALUES (?, ?)', 'arg3', 'arg4');
        $all = $db->allSync('SELECT * FROM t ORDER BY t1 ASC');
        $_ = [['t1' => 'arg1', 't2' => 'arg2'], ['t1' => 'arg3', 't2' => 'arg4']];
        $this->assertEquals($_, $all);
        $one = $db->oneSync('SELECT * FROM t ORDER BY t1 ASC');
        $_ = ['t1' => 'arg1', 't2' => 'arg2'];
        $this->assertEquals($_, $one);
        $col = $db->colSync('SELECT t1 FROM t ORDER BY t1 ASC');
        $_ = ['arg1', 'arg3'];
        $this->assertEquals($_, $col);
        $val = $db->valSync('SELECT t1 FROM t ORDER BY t1 ASC LIMIT 1');
        $_ = 'arg1';
        $this->assertEquals($_, $val);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_tables()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t1 ()');
        $db->execSync('CREATE TABLE t2 ()');
        $this->assertTrue(in_array('t1', $db->tablesSync()));
        $this->assertTrue(in_array('t2', $db->tablesSync()));
        $this->assertTrue($db->tableExistsSync('t1'));
        $this->assertTrue($db->tableExistsSync('t2'));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_drop()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t1 ()');
        $this->assertTrue($db->tableExistsSync('t1'));
        $db->dropSync('t1');
        $this->assertTrue(!$db->tableExistsSync('t1'));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_count_truncate()
    {
        $db = $this->databasePool->random();
        $this->assertTrue($db->countSync('t1') === null);
        $db->execSync('CREATE TABLE t1 ()');
        $db->execSync('INSERT INTO t1 DEFAULT VALUES');
        $db->execSync('INSERT INTO t1 DEFAULT VALUES');
        $this->assertTrue($db->countSync('t1') === 2);
        $db->truncateSync('t1');
        $this->assertTrue($db->countSync('t1') === 0);
        $db->execSync('INSERT INTO t1 DEFAULT VALUES');
        $this->assertTrue($db->countSync('t1') === 1);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_count_where()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t1 (i INT)');
        $db->execSync('INSERT INTO t1 (i) VALUES (1), (5), (10), (100)');
        $this->assertTrue($db->countSync('t1') === 4);
        $_ = ['i' => 1];
        $this->assertTrue($db->countSync('t1', new WhereCondition($_)) === 1);
        $_ = ['i' => [1, 5]];
        $this->assertTrue($db->countSync('t1', new WhereCondition($_)) === 2);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_fields()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t1 (t1 TEXT, ti INT, pk1 INT NOT NULL, pk2 INT NOT NULL)');
        $db->execSync('CREATE TABLE t2 (t2 TEXT)');
        $db->execSync('CREATE TABLE t3 ()');
        $db->execSync('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)');
        $this->assertEquals(null, $db->fieldsSync('unknown'));
        $this->assertEquals(null, $db->fieldsSync(''));
        $fields = $db->fieldsSync('t1');
        $this->assertEquals(['t1', 'ti', 'pk1', 'pk2'], $fields);
        $fields = $db->fieldsSync('t2');
        $this->assertEquals(['t2'], $fields);
        $fields = $db->fieldsSync('t3');
        $this->assertEquals([], $fields);
        $this->assertTrue($db->fieldExistsSync('t1', 't1'));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_indexes()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t1 (t1_field TEXT, t2_field INT NOT NULL)');
        $db->execSync('CREATE TABLE t2 (t2 TEXT)');
        $db->execSync('CREATE TABLE t3 ()');
        $db->execSync('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (t2_field)');
        $db->execSync('CREATE INDEX t1_index1 ON t1 (t1_field)');
        $db->execSync('CREATE INDEX t1_index2 ON t1 (t1_field, t2_field)');
        $db->execSync('CREATE INDEX t1_index3 ON t1 (t2_field)');
        $this->assertEquals(null, $db->indexesSync('unknown'));
        $this->assertEquals(null, $db->indexesSync(''));
        $indexes = $db->indexesSync('t1');
        $this->assertTrue($indexes['t1_index1'] === ['t1_field']);
        $this->assertTrue($indexes['t1_index2'] === ['t1_field', 't2_field']);
        $this->assertTrue($indexes['t1_index3'] === ['t2_field']);
        $this->assertTrue(!isset($indexes['pk1_pkey']));
        $this->assertTrue($db->indexExistsSync('t1', 't1_index1'));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_pk()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t0 (t0 TEXT)');
        $db->execSync('CREATE TABLE t1 (t1 TEXT, pk1 INT NOT NULL)');
        $db->execSync('CREATE TABLE t2 (t1 TEXT, pk1 INT NOT NULL, pk2 INT NOT NULL)');
        $db->execSync('ALTER TABLE ONLY t1 ADD CONSTRAINT pk1_pkey PRIMARY KEY (pk1)');
        $db->execSync('ALTER TABLE ONLY t2 ADD CONSTRAINT pk12_pkey PRIMARY KEY (pk1, pk2)');
        $this->assertTrue($db->pkSync('') === null);
        $this->assertTrue($db->pkSync('tt' . mt_rand()) === null);
        $this->assertTrue($db->pkSync('t0') === []);
        $this->assertTrue($db->pkSync('t1') === ['pk1']);
        $this->assertTrue($db->pkSync('t2') === ['pk1', 'pk2']);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_min_max()
    {
        $db = $this->databasePool->random();
        //
        $db->execSync('CREATE TABLE t1 (pk INT NOT NULL)');
        $db->execSync('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk)');
        $this->assertEquals([null], $db->minSync('t1'));
        $this->assertEquals([null], $db->maxSync('t1'));
        $is = range(10, 90);
        shuffle($is);
        foreach ($is as $i) {
            $db->execSync('INSERT INTO t1 (pk) VALUES (?)', $i);
        }
        $this->assertEquals([10], $db->minSync('t1'));
        $this->assertEquals([90], $db->maxSync('t1'));
        //
        $db->execSync('CREATE TABLE t2 (pk1 TEXT NOT NULL, pk2 TEXT NOT NULL)');
        $db->execSync('ALTER TABLE ONLY t2 ADD CONSTRAINT pk12_pkey PRIMARY KEY (pk1, pk2)');
        $this->assertEquals([null, null], $db->minSync('t2'));
        $this->assertEquals([null, null], $db->maxSync('t2'));
        $db->execSync('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '2', 'b');
        $db->execSync('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '3', 'c');
        $db->execSync('INSERT INTO t2 (pk1, pk2) VALUES (?, ?)', '1', 'a');
        $this->assertTrue(!is_int($db->minSync('t2'))[0]);
        $this->assertTrue(!is_int($db->maxSync('t2'))[0]);
        $this->assertEquals(['1', 'a'], $db->minSync('t2'));
        $this->assertEquals(['3', 'c'], $db->maxSync('t2'));
        //
        $db->execSync('CREATE TABLE t3 (pk TEXT NOT NULL)');
        $this->assertEquals([], $db->minSync('t3'));
        //
        $this->assertEquals(null, $db->minSync('t4'));
    }

    /**
     * @test
     */
    public function test_case_database_postgres_create()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id');
        $this->assertTrue($db->tableExistsSync('tt'));
        $this->assertTrue($db->fieldsSync('tt') === ['tt_id']);
        $this->assertTrue($db->pkSync('tt') === ['tt_id']);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_insert()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id', 4, 2);
        $id = $db->insertSync('tt', 'tt_id');
        $this->assertTrue($id === 4);
        $id = $db->insertSync('tt', 'tt_id');
        $this->assertTrue($id === 6);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_update()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id', 1, 1, [new Field('f1')]);
        $this->assertTrue($db->fieldExistsSync('tt', 'f1'));
        $f = new Field('f1');
        $f->setValue('foo');
        $id = $db->insertSync('tt', 'tt_id', [$f]);
        $val = $db->valSync('SELECT f1 FROM tt WHERE tt_id = ?', $id);
        $this->assertEquals('foo', $val);
        $f->setValue('bar');
        $n = $db->updateSync('tt', 'tt_id', [$id], [$f]);
        $this->assertTrue($n === 1);
        $val = $db->valSync('SELECT f1 FROM tt WHERE tt_id = ?', $id);
        $this->assertEquals('bar', $val);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_delete()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id', 1, 1);
        $id = $db->insertSync('tt', 'tt_id');
        $this->assertEquals(1, $db->countSync('tt'));
        $this->assertTrue($id > 0);
        $n = $db->deleteSync('tt', 'tt_id', [$id]);
        $this->assertTrue($n === 1);
        $this->assertEquals(0, $db->countSync('tt'));
        $n = $db->deleteSync('tt', 'tt_id', [$id]);
        $this->assertTrue($n === 0);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_reid()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id', 1, 1);
        $id = $db->insertSync('tt', 'tt_id');
        $this->assertTrue($id === 1);
        $ok = function ($tt_id) use ($db) {
            return !!$db->countSync('tt', new WhereCondition(compact('tt_id')));
        };
        $this->assertTrue($ok(1));
        $_ = $db->reidSync('tt', 'tt_id', 1, 2);
        $this->assertTrue($_);
        $this->assertTrue(!$ok(1));
        $this->assertTrue($ok(2));
        $_ = $db->reidSync('tt', 'tt_id', 1, 2);
        $this->assertTrue(!$_);
    }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_0()
    // {
    //     $db = $this->databasePool->random();
    //     wait($db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)'));
    //     wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)'));
    //     $ids = [];
    //     foreach ([1, 2, 3, 4] as $id) {
    //         wait($db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id));
    //         $ids[] = $id;
    //     }
    //     $result = [];
    //     $params = ['config' => ['iterator_chunk_size' => mt_rand(1, 4)]];
    //     $iterator = $db->iterate('t1', $params);
    //     \Amp\Loop::run(function () use ($iterator, &$result) {
    //         $result[] = (yield $iterator->pull())[0];
    //         $result[] = (yield $iterator->pull())[0];
    //     });
    //     $this->assertTrue($result === [1, 2]);
    //     $result = [];
    //     foreach ($iterator->generator() as $id => $row) {
    //         $result[] = $id;
    //     }
    //     $this->assertTrue($result === [3, 4]);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_1()
    // {
    //     $db = $this->databasePool->random();
    //     wait($db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)'));
    //     wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)'));
    //     $ids = [];
    //     foreach (range(40, 50) as $id) {
    //         wait($db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id));
    //         $ids[] = $id;
    //     }
    //     $_ids = $ids;
    //     $asc = (bool) mt_rand(0, 1);
    //     $params = ['asc' => $asc, 'config' => ['iterator_chunk_size' => mt_rand(1, 4)]];
    //     foreach ($db->iterate('t1', $params)->generator() as $key => $value) {
    //         $k = array_search($key, $_ids);
    //         $this->assertTrue(count($value) === 1);
    //         $this->assertTrue($k !== false);
    //         unset($_ids[$k]);
    //     }
    //     $this->assertTrue($_ids === []);
    //     $this->assertEquals(11, count(iterator_to_array($db->iterate('t1')->generator())));
    //     $this->assertEquals(11, count(iterator_to_array($db->iterate('t1', $params)->generator())));
    //     $_params = ['max' => 49] + $params;
    //     $this->assertEquals(10, count(iterator_to_array($db->iterate('t1', $_params)->generator())));
    //     $_params = ['min' => 42] + $params;
    //     $this->assertEquals(9, count(iterator_to_array($db->iterate('t1', $_params)->generator())));
    //     $_params = ['min' => 42, 'max' => 47] + $params;
    //     $this->assertEquals(6, count(iterator_to_array($db->iterate('t1', $_params)->generator())));
    //     $_params = ['min' => 42, 'max' => 47, 'limit' => 3] + $params;
    //     $this->assertEquals(3, count(iterator_to_array($db->iterate('t1', $_params)->generator())));
    //     $_params = ['fields' => []] + $params;
    //     $_ = iterator_to_array($db->iterate('t1', $_params)->generator());
    //     $this->assertEquals([], current($_));
    //     $this->assertEquals($asc ? 40 : 50, key($_));
    //     $_params = ['asc' => !$asc] + $params;
    //     $_ = iterator_to_array($db->iterate('t1', $_params)->generator());
    //     $this->assertEquals($asc ? 50 : 40, key($_));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_2()
    // {
    //     $db = $this->databasePool->random();
    //     wait($db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)'));
    //     wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)'));
    //     $ids = [];
    //     foreach (range(10, 100) as $id) {
    //         wait($db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id));
    //         $ids[] = $id;
    //     }
    //     $params = ['config' => [
    //         'iterator_chunk_size' => mt_rand(1, 100),
    //         'rand_iterator_intervals' => mt_rand(1, 100),
    //     ]];
    //     $ids = array_keys(iterator_to_array($db->iterate('t1', ['rand' => true])->generator()));
    //     $this->assertFalse($ids[0] === 10 && $ids[count($ids) - 1] === 100);
    //     $this->assertFalse($ids[0] === 100 && $ids[count($ids) - 1] === 10);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_iterate_3()
    // {
    //     $db = $this->databasePool->random();
    //     wait($db->exec('CREATE TABLE t1 (pk1 TEXT NOT NULL)'));
    //     wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)'));
    //     $ids = [];
    //     foreach (range('a', 'z') as $id) {
    //         wait($db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id));
    //         $ids[] = $id;
    //     }
    //     $params = ['config' => ['iterator_chunk_size' => mt_rand(2, 4)]];
    //     $ids = array_keys(iterator_to_array($db->iterate('t1', $params)->generator()));
    //     $this->assertTrue($ids[0] === 'a' && $ids[count($ids) - 1] === 'z');
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_rand_intervals()
    // {
    //     $db = $this->databasePool->random();
    //     //
    //     $intervals = $this->call($db, 'getIntervalsForRandIterator', 1, 3, 3);
    //     $this->assertEquals([[1, 1], [2, 2], [3, 3]], $intervals);
    //     $intervals = $this->call($db, 'getIntervalsForRandIterator', 1, 3, 30);
    //     $this->assertEquals([1, 1], $intervals[0]);
    //     $this->assertEquals([3, 3], $intervals[count($intervals) - 1]);
    //     $intervals = $this->call($db, 'getIntervalsForRandIterator', 1, 30, 10);
    //     $this->assertEquals([1, 3], $intervals[0]);
    //     $this->assertEquals([28, 30], $intervals[count($intervals) - 1]);
    // }

    

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_get()
    // {
    //     $db = $this->databasePool->random();
    //     wait($db->exec('CREATE TABLE t1 (pk1 INT NOT NULL)'));
    //     wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)'));
    //     foreach (range(1, 1000) as $id) {
    //         wait($db->exec('INSERT INTO t1 (pk1) VALUES (?)', $id));
    //     }
    //     $_ = iterator_to_array($db->get('t1', [1E6])->generator());
    //     $this->assertTrue($_ === []);
    //     $_ = iterator_to_array($db->get('t2', [1E6])->generator());
    //     $this->assertTrue($_ === []);
    //     $_ = iterator_to_array($db->get('t1', [1, 2], ['fields' => []])->generator());
    //     $this->assertTrue($_ === [1 => [], 2 => []]);
    //     $_ = iterator_to_array($db->get('t1', range(1, 1000), ['fields' => []])->generator());
    //     $this->assertTrue($_ === array_fill_keys(range(1, 1000), []));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_database_postgres_get_fields()
    // {
    //     $db = $this->databasePool->random();
    //     wait($db->exec('CREATE TABLE t1 (pk1 INT NOT NULL, "text" TEXT NOT NULL)'));
    //     wait($db->exec('ALTER TABLE ONLY t1 ADD CONSTRAINT pk_pkey PRIMARY KEY (pk1)'));
    //     $string = '"foo"';
    //     wait($db->exec('INSERT INTO t1 (pk1, "text") VALUES (?, ?)', 1, $string));
    //     //
    //     $fields = [new Field('text')];
    //     $_ = iterator_to_array($db->get('t1', [1], compact('fields'))->generator());
    //     $_ = current($_);
    //     $this->assertTrue(isset($_['text']));
    //     //
    //     $fields = [new Field('text', null, 'text1')];
    //     $_ = iterator_to_array($db->get('t1', [1], compact('fields'))->generator());
    //     $_ = current($_);
    //     $this->assertTrue(isset($_['text1']));
    //     //
    //     $fields = [new Field('text', Type::json(), 'json')];
    //     $_ = iterator_to_array($db->get('t1', [1], compact('fields'))->generator());
    //     $_ = current($_);
    //     $this->assertEquals($_, ['json' => 'foo']);
    // }
}
