<?php

namespace Tests;

use Ejz\Type;
use Ejz\DatabaseBean;
use Ejz\BitmapBean;
use Ejz\Index;
use Ejz\Iterator;
use Ejz\Repository;
use Amp\Promise;
use RuntimeException;
use Ejz\WhereCondition;

class TestCaseRepository extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_repository_cluster_0()
    {
        $n2i = function ($names, $all) {
            return implode(',', array_map(function ($name) use ($all) {
                return array_search($name, $all);
            }, $names));
        };
        $cases = [
            '' => ['', ''],
            'm:*;s:*;' => ['0,1,2', ''],
            'm:1;s:*;' => ['1', '0,2'],
            'm:!1;s:!2;' => ['0,2', '1'],
        ];
        foreach ($cases as $cluster => [$m, $s]) {
            $repository = \Container\getRepository('t', ['database' => compact('cluster')]);
            $all = $repository->getDatabasePool()->names();
            $names = $repository->getMasterDatabasePool()->names();
            $this->assertEquals($m, $n2i($names, $all));
            $names = $repository->getSlaveDatabasePool()->names();
            $this->assertEquals($s, $n2i($names, $all));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_cluster_1()
    {
        $n2i = function ($names, $all) {
            return implode(',', array_map(function ($name) use ($all) {
                return array_search($name, $all);
            }, $names));
        };
        $cases = [
            'm:*;' => ['1', '0'],
            'm:*;' => ['2', '1'],
            'm:*;' => ['3', '2'],
            'm:*;' => ['4', '0'],
            'm:*;ms:2:id;' => ['1', '0,1'],
            'm:*;ms:2:id;' => ['2', '1,2'],
            'm:*;ms:2:id;' => ['3', '2,0'],
            'm:*;ms:2:id;' => ['4', '0,1'],
            'm:*;ms:*:id;' => ['2', '1,2,0'],
        ];
        foreach ($cases as $cluster => [$id, $m]) {
            $repository = \Container\getRepository('t', ['database' => compact('cluster')]);
            $all = $repository->getDatabasePool()->names();
            $names = $repository->getMasterDatabasePool($id)->names();
            $this->assertEquals($m, $n2i($names, $all));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_cluster_2()
    {
        $n2i = function ($names, $all) {
            return implode(',', array_map(function ($name) use ($all) {
                return array_search($name, $all);
            }, $names));
        };
        $cases = [
            'm:*;ms:1:id;' => '~^0,1,2$~',
            'm:*;ms:3:id;' => '~^[012]$~',
            'm:*;ms:2:id;' => '~^[01],[12]$~',
        ];
        foreach ($cases as $cluster => $regex) {
            $repository = \Container\getRepository('t', ['database' => compact('cluster')]);
            $all = $repository->getDatabasePool()->names();
            $names = $repository->getReadableMasterDatabasePool()->names();
            $this->assertRegExp($regex, $n2i($names, $all));
            $this->assertTrue($repository->getReadableSlaveDatabasePool()->names() === []);
        }
    }

    /**
     * @test
     */
    public function test_case_repository_create_0()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;',
            ],
        ]);
        $repository->createSync();
        $db = $repository->getDatabasePool()->random();
        $this->assertTrue($db->tableExistsSync('t'));
        $this->assertTrue(!$db->tableExistsSync('tt'));
    }

    /**
     * @test
     */
    public function test_case_repository_create_1()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'pk' => 'foo',
                'fields' => [
                    'f1' => Type::string(),
                ],
            ],
        ]);
        $repository->createSync();
        $master = $repository->getMasterDatabasePool()->random();
        $slave = $repository->getSlaveDatabasePool()->random();
        $this->assertTrue($master->fieldExistsSync('t', 'foo'));
        $this->assertTrue($slave->fieldExistsSync('t', 'foo'));
        $this->assertTrue($master->fieldExistsSync('t', 'f1'));
        $this->assertTrue(!$slave->fieldExistsSync('t', 'f1'));
    }

    /**
     * @test
     */
    public function test_case_repository_create_2()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'f1' => Type::string(),
                    'f2' => Type::string(),
                ],
                'indexes' => [
                    'f1' => ['f1'],
                    'f2' => ['f1', 'f2'],
                ],
            ],
        ]);
        $repository->createSync();
        $master = $repository->getMasterDatabasePool()->random();
        $indexes = $master->indexesSync('t');
        $indexes = array_map(function ($fields) {
            return implode(',', $fields);
        }, $indexes);
        $indexes = array_flip($indexes);
        $this->assertTrue(isset($indexes['f1']));
        $this->assertTrue(isset($indexes['f1,f2']));
    }

    /**
     * @test
     */
    public function test_case_repository_create_3()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'table' => 't',
                'fields' => [
                    'f1' => Type::string(),
                ],
                'indexes' => [
                    'f1' => [
                        'fields' => ['f1'],
                        'type' => Index::INDEX_TYPE_UNIQUE,
                    ],
                ],
            ],
        ]);
        $repository->createSync();
        $master = $repository->getMasterDatabasePool()->random();
        $indexes = $master->indexesSync('t');
        $indexes = array_map(function ($fields) {
            return implode(',', $fields);
        }, $indexes);
        $indexes = array_flip($indexes);
        $this->assertTrue(isset($indexes['f1']));
        $master->execSync('INSERT INTO t (f1) VALUES (1)');
        Promise\wait(Promise\any([$master->exec('INSERT INTO t (f1) VALUES (1)')]));
        $c = $master->countSync('t');
        $this->assertTrue($c === 1);
    }

    /**
     * @test
     */
    public function test_case_repository_create_4()
    {
        $parent = \Container\getRepository('parent', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'pk' => 'parent_id',
                'fields' => [
                    'text' => Type::string(true),
                ],
            ],
        ]);
        $child = \Container\getRepository('child', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'pk' => 'child_id',
                'fields' => [
                    'parent_id' => [
                        'type' => Type::bigInt(),
                        'slave' => true,
                    ],
                ],
                'fks' => [
                    'parent_id' => 'parent.parent_id',
                ],
            ],
        ]);
        $parent->createSync();
        $child->createSync();
        $master = $parent->getMasterDatabasePool()->random();
        $master->execSync('INSERT INTO "parent" ("parent_id") VALUES (1)');
        $master->execSync('INSERT INTO "parent" ("parent_id") VALUES (2)');
        $master->execSync('INSERT INTO "child" ("child_id", "parent_id") VALUES (1, 1)');
        $master->execSync('INSERT INTO "child" ("child_id", "parent_id") VALUES (2, 2)');
        $master->execSync('INSERT INTO "child" ("child_id", "parent_id") VALUES (3, 2)');
        //
        $promise = $master->exec('INSERT INTO "child" ("child_id", "parent_id") VALUES (4, 3)');
        Promise\wait(Promise\any([$promise]));
        $this->assertTrue($master->countSync('child') === 3);
        //
        $promise = $master->exec('UPDATE "child" SET "parent_id" = 3 WHERE "parent_id" = 2');
        Promise\wait(Promise\any([$promise]));
        $where = new WhereCondition(['parent_id' => 3]);
        $this->assertTrue($master->countSync('child', $where) === 0);
        //
        $master->execSync('UPDATE "parent" SET "parent_id" = 3 WHERE "parent_id" = 2');
        $where = new WhereCondition(['parent_id' => 3]);
        $this->assertTrue($master->countSync('child', $where) === 2);
    }

    /**
     * @test
     */
    public function test_case_repository_crud_0()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'field1' => Type::string(),
                    'field2' => Type::string(true),
                    'field3' => Type::int(),
                ],
            ],
        ]);
        $repository->createSync();
        $id1 = $repository->insertSync();
        $id2 = $repository->insertSync();
        $id3 = $repository->insertSync();
        $id4 = $repository->insertSync();
        $id5 = $repository->insertSync();
        $keys = array_keys(iterator_to_array($repository->get([$id5, $id2, $id1, $id5, $id4, $id3])));
        $this->assertEquals([$id5, $id2, $id1, $id4, $id3], $keys);
        $bean = $repository->get([$id1])->current();
        $this->assertTrue(is_object($bean));
        $values = $bean->getValues();
        $this->assertEquals(['field1' => '', 'field2' => null, 'field3' => 0], $values);
    }

    /**
     * @test
     */
    public function test_case_repository_crud_1()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'field1' => Type::string(),
                    'field2' => Type::int(),
                ],
            ],
        ]);
        $repository->createSync();
        $id = $repository->insertSync();
        $bean = $repository->get([$id])->current();
        $this->assertTrue($bean->field1 === '');
        $this->assertTrue($bean->field2 === 0);
        $bean->field1 = '1';
        $bean->field2 = '10';
        $this->assertTrue($bean->field1 === '1');
        $this->assertTrue($bean->field2 === 10);
        $this->assertTrue($bean->getValues()['field1'] === '1');
        $this->assertTrue($bean->getValues()['field2'] === 10);
        $this->assertTrue($bean->updateSync());
        $this->assertFalse($bean->updateSync());
        unset($bean);
        $bean = $repository->get([$id])->current();
        $this->assertTrue($bean->field1 === '1');
        $this->assertTrue($bean->field2 === 10);
    }

    /**
     * @test
     */
    public function test_case_repository_crud_2()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $repository->createSync();
        $id = $repository->insertSync(['field1' => 'foo']);
        $bean = $repository->get([$id])->current();
        $this->assertTrue($bean->field1 === 'foo');
        $this->assertTrue($bean->deleteSync());
        $this->assertFalse($bean->deleteSync());
        $_ = iterator_to_array($repository->get([$id]));
        $this->assertTrue($_ === [$id => null]);
        $id = $repository->insertSync();
        $names = $repository->getWritableDatabasePool($id)->names();
        $this->assertTrue($repository->deleteSync([$id]) === count($names));
    }

    /**
     * @test
     */
    public function test_case_repository_crud_3()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
            ],
        ]);
        $repository->createSync();
        $id1 = $repository->insertSync();
        $id2 = $repository->insertSync();
        $keys = array_keys(iterator_to_array($repository->get([$id1, 1E6, $id2])));
        $this->assertEquals([$id1, (int) 1E6, $id2], $keys);
        $vals = iterator_to_array($repository->get([1E6, $id1, 1E6, $id2, 1E6]), false);
        $this->assertTrue($vals[0] === null);
        $this->assertTrue($vals[1] !== null);
        $this->assertTrue($vals[2] === null);
        $this->assertTrue($vals[3] !== null);
        $this->assertTrue($vals[4] === null);
    }

    /**
     * @test
     */
    public function test_case_repository_crud_4()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text' => Type::string(),
                ],
            ],
        ]);
        $index = $repository->getBitmapIndex();
        $repository->createSync();
        $id = $repository->insertSync(['text' => 'foo']);
        foreach ($repository->iterate() as $id => $bean) {
            $this->assertTrue(is_object($bean));
            $this->assertTrue(is_array($bean->getValues()));
        }
        foreach ($repository->get([$id, 1E6]) as $id => $bean) {
            $this->assertTrue($bean === null || is_object($bean));
            $this->assertTrue($bean === null || is_array($bean->getValues()));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_json_type()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'json' => Type::json(),
                ],
            ],
        ]);
        $repository->createSync();
        $values = [
            'string',
            ['string'],
            ['foo' => 'bar'],
        ];
        foreach ($values as $value) {
            $id = $repository->insertSync(['json' => $value]);
            $bean = $repository->get([$id])->current();
            $this->assertTrue(serialize($bean->json) === serialize($value));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_binary()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'binary' => Type::binary(),
                ],
            ],
        ]);
        $repository->createSync();
        $values = [
            '',
            chr(0),
            str_repeat(chr(1), 1E7) . str_repeat(chr(2), 1E7),
        ];
        foreach ($values as $value) {
            $id = $repository->insertSync(['binary' => $value]);
            $bean = $repository->get([$id])->current();
            $this->assertTrue(serialize($bean->binary) === serialize($value));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_string_array()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'array' => Type::stringArray(),
                ],
            ],
        ]);
        $repository->createSync();
        $values = [
            [['foo']],
            [['']],
            [[true], ['1']],
            [[false], ['']],
            [[null], ['']],
            [['foo' => 'bar'], ['bar']],
            [array_fill(0, 1E4, true), array_fill(0, 1E4, '1')],
            [[]],
        ];
        foreach ($values as $value) {
            $value0 = $value[0];
            $value1 = $value[1] ?? $value0;
            $id = $repository->insertSync(['array' => $value0]);
            $bean = $repository->get([$id])->current();
            $this->assertTrue(serialize($bean->array) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_int_array()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'array' => Type::intArray(),
                ],
            ],
        ]);
        $repository->createSync();
        $values = [
            [[0]],
            [[''], [0]],
            [[true], [1]],
            [[false], [0]],
            [[null], [0]],
            [[1, 2], [1, 2]],
            [range(1, 1E3)],
            [[]],
        ];
        foreach ($values as $value) {
            $value0 = $value[0];
            $value1 = $value[1] ?? $value0;
            $value1 = array_map('intval', $value1);
            $id = $repository->insertSync(['array' => $value0]);
            $bean = $repository->get([$id])->current();
            $this->assertTrue(serialize($bean->array) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_enum()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:0;s:*;',
                'fields' => [
                    'enum' => Type::enum(['foo', 'bar']),
                ],
            ],
        ]);
        $repository->createSync();
        $id = $repository->insertSync();
        $bean = $repository->get([$id])->current();
        $this->assertTrue($bean->enum === 'foo');
        $values = [
            ['foo'],
            ['bar'],
            ['', 'foo'],
            [[], 'foo'],
        ];
        foreach ($values as $value) {
            $value0 = $value[0];
            $value1 = $value[1] ?? $value0;
            $id = $repository->insertSync(['enum' => $value0]);
            $bean = $repository->get([$id])->current();
            $this->assertTrue(serialize($bean->enum) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_repository_iterate_0()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text1' => Type::string(),
                ],
            ],
        ]);
        $repository->createSync();
        //
        $ids = [];
        [$first, $last] = [1, 2000];
        foreach (range($first, $last) as $_) {
            $ids[] = $repository->insertSync();
        }
        sort($ids);
        $this->assertEquals(range(1, 1000), array_slice($ids, 0, 1000));
        //
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 20);
        $params = ['config' => compact('iterator_chunk_size')];
        foreach ($repository->iterate($params) as $bean) {
            $_ids[] = $bean->id;
        }
        $this->assertEquals($ids, $_ids);
        //
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 20);
        $params = ['config' => compact('iterator_chunk_size')];
        foreach ($repository->iterate($params) as $bean) {
            $_ids[] = $bean->id;
        }
        $this->assertEquals($ids, $_ids);
        //
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 20);
        $params = ['config' => compact('iterator_chunk_size'), 'asc' => false];
        foreach ($repository->iterate($params) as $bean) {
            $_ids[] = $bean->id;
        }
        arsort($ids);
        $ids = array_values($ids);
        $this->assertEquals($ids, $_ids);
    }

    /**
     * @test
     */
    public function test_case_repository_filter()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text1' => Type::string(),
                ],
            ],
        ]);
        $repository->createSync();
        foreach (range(1, 1000) as $_) {
            $repository->insertSync(['text1' => mt_rand(1, 3)]);
        }
        $this->assertTrue(
            count(iterator_to_array($repository->filter(['text1' => 1]))) +
            count(iterator_to_array($repository->filter(['text1' => 2]))) +
            count(iterator_to_array($repository->filter(['text1' => 3]))) ===
            1000
        );
        $this->assertTrue($repository->existsSync(['text1' => 1]));
        $this->assertTrue(!$repository->existsSync(['text1' => 10]));
    }

    /**
     * @test
     */
    public function test_case_repository_reid()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text1' => Type::string(),
                ],
            ],
        ]);
        $repository->createSync();
        $id = $repository->insertSync(['text1' => 'foo']);
        $this->assertTrue($repository->reidSync($id, $id + 1));
        $null = $repository->get([$id])->current();
        $this->assertTrue($null === null);
        $bean = $repository->get([$id + 1])->current();
        $this->assertTrue($bean->text1 === 'foo');
    }

    /**
     * @test
     */
    public function test_case_repository_sort_chains()
    {
        $repository = \Container\getRepository('t', []);
        $result = $this->call($repository, 'getSortChains', [1 => 1, 2 => 2]);
        $this->assertTrue($result[0] === [2, 1]);
        $result = $this->call($repository, 'getSortChains', [1 => 1, 2 => 3, 3 => 2]);
        $this->assertTrue($result[0] === [3, 2, 1]);
        $result = $this->call($repository, 'getSortChains', [1 => 1, 2 => 3, 3 => 2, 10 => 10, 11 => 11]);
        $this->assertTrue($result[0] === [11, 1]);
        $this->assertTrue($result[1] === [3, 10, 2]);
    }

    /**
     * @test
     */
    public function test_case_repository_table_sort()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text1' => Type::int(),
                ],
            ],
            'getSortScore' => function ($bean) {
                return $bean->text1;
            },
        ]);
        $repository->createSync();
        $text1s = [];
        foreach (range(1, array_rand(array_flip([10, 100, 1000]))) as $_) {
            $_ = mt_rand(1, 100);
            $text1s[] = $_;
            $repository->insertSync(['text1' => $_]);
        }
        $this->assertTrue($repository->sortSync());
        // return;
        // $min = min($text1s);
        // $max = max($text1s);
        // $this->assertTrue($min < $max);
        // $collect = [[], []];
        // foreach ($storage->getDatabasePool()->names() as $name) {
        //     $gen = $repository->iterate(['asc' => true, 'poolFilter' => $name]);
        //     $collect[0][] = $gen->current()->text1;
        //     $gen = $repository->iterate(['asc' => false, 'poolFilter' => $name]);
        //     $collect[1][] = $gen->current()->text1;
        // }
        // $this->assertEquals(min($collect[1]), $min);
        // $this->assertEquals(max($collect[0]), $max);
    }

    /**
     * @test
     */
    public function test_case_repository_cache_0()
    {
        $ttl = 3;
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text1' => Type::string(),
                ],
            ],
            'cache' => [
                'ttl' => $ttl,
            ],
        ]);
        $pk = $repository->getDatabasePk();
        $repository->createSync();
        $id = $repository->insertSync(['text1' => 'foo']);
        $text1 = $repository->get([$id])->current()->text1;
        $this->assertTrue($text1 === 'foo');
        $pool = $repository->getDatabasePool();
        $pool->execSync("UPDATE t SET text1 = ? WHERE {$pk} = ?", 'bar', $id);
        $text1 = $repository->get([$id])->current()->text1;
        $this->assertTrue($text1 === 'foo');
        sleep($ttl - 1);
        $text1 = $repository->get([$id])->current()->text1;
        $this->assertTrue($text1 === 'foo');
        sleep(2);
        $text1 = $repository->get([$id])->current()->text1;
        $this->assertTrue($text1 === 'bar');
    }

    /**
     * @test
     */
    public function test_case_repository_cache_1()
    {
        $ttl = 3;
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'text1' => Type::string(),
                ],
            ],
            'cache' => [
                'ttl' => $ttl,
                'cacheableFields' => ['text1'],
            ],
        ]);
        $pk = $repository->getDatabasePk();
        $repository->createSync();
        $id = $repository->insertSync(['text1' => 'foo1']);
        $text1 = $repository->get([$id])->current()->text1;
        $this->assertTrue($text1 === 'foo1');
        $text1 = $repository->filter(['text1' => 'foo1'])->current()->text1;
        $this->assertTrue($text1 === 'foo1');
        $sql = "UPDATE t SET text1 = ? WHERE {$pk} = ?";
        $repository->getDatabasePool()->execSync($sql, 'bar1', $id);
        $text1 = $repository->filter(['text1' => 'foo1'])->current()->text1;
        $this->assertTrue($text1 === 'foo1');
        sleep($ttl + 1);
        $this->assertTrue($repository->filter(['text1' => 'foo1'])->current() === null);
    }

    /**
     * @test
     */
    public function test_case_repository_bitmap_0()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'boolean' => Type::string(),
                ],
            ],
            'bitmap' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'boolean' => Type::bitmapBool(),
                ],
            ],
        ]);
        $index = $repository->getBitmapIndex();
        $repository->createSync();
        $id = $repository->addSync(1E6);
        $this->assertTrue($id === (int) 1E6);
        $indexes = $repository->getBitmapPool()->indexesSync();
        $this->assertTrue(in_array($index, current($indexes)));
        foreach (range(1, 100) as $_) {
            $repository->insertSync(['boolean' => mt_rand(0, 1)]);
        }
        $repository->populateBitmap();
        $ex = null;
        foreach ($repository->search('*') as $id => $bean) {
            $ex = $ex ?? $id;
            $this->assertTrue($id >= $ex);
            $ex = $id;
        }
        $ex = null;
        foreach ($repository->search('*', ['asc' => false]) as $id => $bean) {
            $ex = $ex ?? $id;
            $this->assertTrue($id <= $ex);
            $ex = $id;
        }
        foreach ($repository->search('@boolean:1') as $id => $bean) {
            $this->assertTrue(((bool) $bean->boolean) === true);
        }
        foreach ($repository->search('@boolean:0') as $id => $bean) {
            $this->assertTrue(((bool) $bean->boolean) === false);
        }
        $_0 = $repository->search('@boolean:0')->getContext()['size'];
        $_1 = $repository->search('@boolean:1')->getContext()['size'];
        $this->assertTrue($_0 + $_1 === 100);
        $this->assertTrue($_0 > 10 && $_1 > 10);
    }

    /**
     * @test
     */
    public function test_case_repository_bitmap_1()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:1:id;',
                'fields' => [
                    'int' => Type::int(),
                ],
            ],
            'bitmap' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'int' => Type::bitmapInt(),
                ],
            ],
        ]);
        $repository->createSync();
        $values = [];
        foreach (range(1, 1000) as $_) {
            $int = mt_rand(1, 100);
            $id = $repository->insertSync(['int' => $int]);
            $values[] = [$id, $int];
        }
        $idAsc = function ($filter = null) use ($values) {
            if ($filter !== null) {
                $values = array_filter($values, $filter);
            }
            uasort($values, function ($a, $b) {
                [$id1, $val1] = $a;
                [$id2, $val2] = $b;
                return $id1 - $id2;
            });
            return array_values($values);
        };
        $valSort = function ($filter = null, $asc = true) use ($values) {
            if ($filter !== null) {
                $values = array_filter($values, $filter);
            }
            uasort($values, function ($a, $b) use ($asc) {
                [$id1, $val1] = $a;
                [$id2, $val2] = $b;
                return (($asc ? 1 : -1) * ($val1 - $val2)) ?: $id1 - $id2;
            });
            return array_values($values);
        };
        $repository->populateBitmap();
        //
        $collect = [];
        foreach ($repository->search('*') as $id => $bean) {
            $collect[] = [$id, $bean->int];
        }
        $this->assertEquals($idAsc(), $collect);
        //
        $collect = [];
        foreach ($repository->search('*', ['asc' => false]) as $id => $bean) {
            $collect[] = [$id, $bean->int];
        }
        $this->assertEquals(array_reverse($idAsc()), $collect);
        //
        $collect = [];
        foreach ($repository->search('*', ['sortby' => 'int']) as $id => $bean) {
            $collect[] = [$id, $bean->int];
        }
        $this->assertEquals($valSort(), $collect);
        //
        $collect = [];
        foreach ($repository->search('*', ['sortby' => 'int', 'asc' => false]) as $id => $bean) {
            $collect[] = [$id, $bean->int];
        }
        $this->assertEquals($valSort(null, false), $collect);
        //
        $collect = [];
        $iterator = $repository->search('*', ['sortby' => 'int', 'asc' => false]);
        while ($iterator->advanceSync()) {
            [$id, $bean] = $iterator->getCurrent();
            $collect[] = [$id, $bean->int];
        }
        $this->assertEquals($valSort(null, false), $collect);
    }

    /**
     * @test
     */
    public function test_case_repository_bitmap_2()
    {
        $pool = \Container\getRepositoryPool(['t' => [
            'database' => [
                'cluster' => 'm:*;ms:1:id,int;',
                'fields' => [
                    'int' => Type::int(),
                    'fk' => Type::int(),
                ],
            ],
            'bitmap' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'int' => Type::bitmapInt(),
                    'fk' => Type::bitmapForeignKey('t'),
                ],
            ],
        ]]);
        $repository = $pool->get('t');
        $repository->createSync();
        foreach (range(1, 100) as $_) {
            $repository->insertSync(['int' => mt_rand(1, 100), 'fk' => mt_rand(1, 100)]);
        }
        $repository->populateBitmap();
        $iterator = $repository->search('*', ['sortby' => 'int', 'asc' => false, 'fks' => 'fk']);
        while ($iterator->advanceSync()) {
            [$id1, $bean1, $id2, $bean2] = $iterator->getCurrent();
            $this->assertTrue($bean1->fk === $id2);
        }
    }

    /**
     * @test
     */
    public function test_case_repository_insert_unique()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;ms:*:id;',
                'fields' => [
                    'int' => Type::int(),
                ],
                'indexes' => [
                    'f1' => [
                        'fields' => ['int'],
                        'type' => Index::INDEX_TYPE_UNIQUE,
                    ],
                ],
            ],
        ]);
        $repository->createSync();
        $table = $repository->getDatabaseTable();
        $id = $repository->insertSync(['int' => 1]);
        $this->assertTrue($id > 0);
        $id = $repository->insertSync(['int' => 1]);
        $this->assertTrue($id === 0);
    }

    /**
     * @test
     */
    public function test_case_repository_get_if_no_database()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
            ],
            'bitmap' => [
                'cluster' => 'm:*;',
            ],
        ]);
        $repository->createSync();
        $repository->addSync(1);
        foreach ($repository->search('*') as $id => $bean) {
            $this->assertTrue($id > 0 && $bean === null);
        }
        foreach (Iterator::wrap($repository->search('*')) as $id => $beans) {
            $this->assertTrue($id > 0 && $beans === [null] && $beans[0] === null);
        }
    }

    /**
     * @test
     */
    public function test_case_repository_get_order()
    {
        $repository = \Container\getRepository('t', [
            'database' => [
                'cluster' => 'm:*;',
            ],
        ]);
        $repository->createSync();
        $id1 = $repository->insertSync();
        $id2 = $repository->insertSync();
        $ids = [];
        foreach ($repository->get([$id2, 1E6, $id1, 1E6, $id2]) as $_ => $value) {
            $ids[] = $_;
        }
        $this->assertTrue([$id2, (int) 1E6, $id1, (int) 1E6, $id2] === $ids);
    }
}
