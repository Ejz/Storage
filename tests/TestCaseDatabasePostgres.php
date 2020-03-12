<?php

namespace Tests;

use Ejz\Field;
use Ejz\DatabaseType;
use Ejz\WhereCondition;
use Ejz\DatabasePostgresException;

class TestCaseDatabasePostgres extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_database_postgres_connection_parse_array()
    {
        $db = $this->databasePool->random();
        $db->execSync('SELECT 1');
        $connection = $this->getPrivateProperty($db, 'connection');
        $cases = [
            '' => [],
            '{}' => [],
            '{asd}' => ['asd'],
            '{foo,bar}' => ['foo', 'bar'],
            '{"foo","bar"}' => ['foo', 'bar'],
            '{{foo,bar},foo,bar}' => [['foo', 'bar'], 'foo', 'bar'],
            '{{foo,bar},{"foo"},{"bar"}}' => [['foo', 'bar'], ['foo'], ['bar']],
            '{"\\"","\\\\"}' => ['"', '\\'],
            '{NULL,"NULL"}' => [null, 'NULL'],
        ];
        foreach ($cases as $parse => $assert) {
            [$result] = $this->callPrivateMethod($connection, 'parseArray', $parse);
            $this->assertEquals($assert, $result);
        }
    }

    /**
     * @test
     */
    public function test_case_database_postgres_connection_substitute()
    {
        $db = $this->databasePool->random();
        $db->execSync('SELECT 1');
        $connection = $this->getPrivateProperty($db, 'connection');
        $cases = [
            ['SELECT ?', [1], 'SELECT 1'],
            ['SELECT ?', [null], 'SELECT NULL'],
            ['SELECT ?', [1.2], 'SELECT 1.2'],
            ['SELECT ??', [], 'SELECT ?'],
            ['SELECT ???', [1], 'SELECT ?1'],
            ['SELECT ?', [[1, 2]], 'SELECT \'{1,2}\''],
            ['SELECT ?', [[null]], 'SELECT \'{NULL}\''],
            ['SELECT #', ['table'], 'SELECT "table"'],
            ['SELECT %', ['foo'], 'SELECT foo'],
            ['SELECT 1 %% 2', [], 'SELECT 1 % 2'],
            ['nextval(?::regclass)', ['seq'], 'nextval(\'seq\'::regclass)'],
            ['SELECT $', ['f'], 'SELECT \'\\x66\''],
        ];
        foreach ($cases as [$substitute, $args, $assert]) {
            $result = $this->callPrivateMethod($connection, 'substitute', $substitute, $args);
            $this->assertEquals($assert, $result);
        }
    }

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
        $this->expectException(DatabasePostgresException::class);
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
        $cases = [
            [null, 4],
            [new WhereCondition(['i' => 1]), 1],
            [new WhereCondition(['i' => [1, 5]]), 2],
            [new WhereCondition([['i', 1, '!=']]), 3],
            [new WhereCondition([['i', 5, '%', '$field $operation $value = 0']]), 3],
            [new WhereCondition([['i', 5, '%', '$field $operation $value != 0']]), 1],
        ];
        foreach ($cases as [$where, $assert]) {
            $this->assertEquals($assert, $db->countSync('t1', $where));
        }
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
        $f = new Field('f1');
        $db->createSync('tt', 'tt_id', 1, 1, [$f]);
        $this->assertTrue($db->fieldExistsSync('tt', 'f1'));
        $f = new Field('f1');
        $f->setValue('foo');
        $id = $db->insertSync('tt', 'tt_id', null, [$f]);
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

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_0()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id');
        foreach (range(1, 60) as $_) {
            $db->insertSync('tt', 'tt_id');
        }
        $asc = !!mt_rand(0, 1);
        $params = ['asc' => $asc, 'config' => ['iterator_chunk_size' => mt_rand(1, 3)]];
        $values = iterator_to_array($db->iterate('tt', $params));
        $keys = array_map(function ($value) {
            return $value[0];
        }, $values);
        $_ = $asc ? range(1, 60) : array_reverse(range(1, 60));
        $this->assertEquals($_, $keys);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_1()
    {
        $db = $this->databasePool->random();
        $f = new Field('str');
        $db->createSync('tt', 'tt_id', 1, 1, [$f]);
        foreach (range(1, 500) as $_) {
            $f->setValue(mt_rand(1, 10));
            $db->insertSync('tt', 'tt_id', null, [$f]);
        }
        $params = ['where' => ['str' => (string) mt_rand(1, 10)]];
        $cnt = count(iterator_to_array($db->iterate('tt', $params)));
        $this->assertTrue(10 <= $cnt && $cnt <= 90);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_2()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id');
        foreach (range(1, 500) as $_) {
            $db->insertSync('tt', 'tt_id');
        }
        $min = mt_rand(1, 500);
        $max = mt_rand($min, 500);
        foreach (range(1, 100) as $_) {
            $config = [
                'iterator_chunk_size' => mt_rand(1, 30),
            ];
            $params = compact('min', 'max', 'config');
            $values = iterator_to_array($db->iterate('tt', $params));
            $keys = array_map(function ($value) {
                return $value[0];
            }, $values);
            $this->assertEquals(range($min, $max), $keys);
        }
    }

    /**
     * @test
     */
    public function test_case_database_postgres_iterate_3()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id');
        foreach (range(1, 500) as $_) {
            $db->insertSync('tt', 'tt_id');
        }
        $min = mt_rand(1, 100);
        $max = mt_rand($min, 250);
        $config = [
            'iterator_chunk_size' => mt_rand(10, 30),
            'rand_iterator_intervals' => mt_rand(2, 30),
        ];
        $params = compact('min', 'max', 'config') + ['asc' => null];
        $values = iterator_to_array($db->iterate('tt', $params));
        $keys = array_map(function ($value) {
            return $value[0];
        }, $values);
        $this->assertNotEquals(range($min, $max), $keys);
        $this->assertNotEquals(array_reverse(range($min, $max)), $keys);
        sort($keys);
        $this->assertEquals(range($min, $max), $keys);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_get()
    {
        $db = $this->databasePool->random();
        $db->createSync('tt', 'tt_id');
        $id1 = $db->insertSync('tt', 'tt_id');
        $id2 = $db->insertSync('tt', 'tt_id');
        $id3 = $db->insertSync('tt', 'tt_id');
        $values = iterator_to_array($db->get('tt', [$id2, $id3, $id1]));
        $keys = array_map(function ($value) {
            return $value[0];
        }, $values);
        $this->assertTrue($keys === [$id2, $id3, $id1]);
    }

    /**
     * @test
     */
    public function test_case_database_postgres_rand_intervals()
    {
        $db = $this->databasePool->random();
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
