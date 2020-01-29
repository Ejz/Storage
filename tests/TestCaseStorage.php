<?php

namespace Tests;

use Ejz\Type;
use Ejz\Bean;
use Ejz\Index;
use Ejz\Storage;
use Amp\Promise;
use RuntimeException;

class TestCaseStorage extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_storage_create_1()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => $_fields = [
                        'string' => Type::string(),
                        'int' => Type::int(),
                        'float' => Type::float(),
                        'bool' => Type::bool(),
                        'date' => Type::date(),
                        'datetime' => Type::dateTime(),
                        'json' => Type::json(),
                        'bigint' => Type::bigInt(),
                        'intarray' => Type::intArray(),
                        'stringarray' => Type::stringArray(),
                        'binary' => Type::binary(),
                    ],
                ],
            ],
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->table();
        $table->createSync();
        $db = $table->getDatabasePool()->random();
        $fields = $db->fieldsSync('table');
        $_fields = array_keys($_fields);
        $_fields[] = $table->getDatabasePk();
        sort($fields);
        sort($_fields);
        $this->assertEquals($_fields, $fields);
    }

    /**
     * @test
     */
    public function test_case_storage_create_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'table' => 'foo',
                ],
            ],
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->table();
        $table->createSync();
        $db = $table->getDatabasePool()->random();
        $this->assertTrue($db->tableExistsSync('foo'));
        $this->assertTrue(!$db->tableExistsSync('table'));
    }

    /**
     * @test
     */
    public function test_case_storage_create_3()
    {
        $storage = \Container\getStorage([
            'Table' => [
                'database' => [
                    'pk' => 'foo',
                ],
            ],
        ]);
        $table = $storage->Table();
        $table->createSync();
        $db = $table->getDatabasePool()->random();
        $fields = $db->fieldsSync('table');
        $this->assertTrue($fields === ['foo']);
    }

    /**
     * @test
     */
    public function test_case_storage_create_4()
    {
        $storage = \Container\getStorage([
            'Table' => [
                'database' => [
                    'fields' => [
                        'f1' => Type::string(),
                        'f2' => Type::string(),
                    ],
                    'indexes' => [
                        'f1' => ['f1'],
                        'f2' => ['f1', 'f2'],
                    ],
                ],
            ],
        ]);
        $table = $storage->Table();
        $table->createSync();
        $db = $table->getDatabasePool()->random();
        $indexes = $db->indexesSync('table');
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
    public function test_case_storage_create_5()
    {
        $storage = \Container\getStorage([
            'Table' => [
                'database' => [
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
            ],
        ]);
        $table = $storage->Table();
        $table->createSync();
        $db = $table->getDatabasePool()->random();
        $indexes = $db->indexesSync('t');
        $indexes = array_map(function ($fields) {
            return implode(',', $fields);
        }, $indexes);
        $indexes = array_flip($indexes);
        $this->assertTrue(isset($indexes['f1']));
        $db->execSync('INSERT INTO t (f1) VALUES (1)');
        Promise\wait(Promise\any([$db->exec('INSERT INTO t (f1) VALUES (1)')]));
        $c = count($db->allSync('SELECT * FROM t'));
        $this->assertTrue($c === 1);
    }

    /**
     * @test
     */
    public function test_case_storage_create_6()
    {
        $storage = \Container\getStorage([
            'parent' => [
                'database' => [
                    'pk' => 'parent_id',
                    'fields' => [
                        'text' => Type::string(true),
                    ],
                ],
            ],
            'child' => [
                'database' => [
                    'pk' => 'id',
                    'fields' => [
                        'parent_id' => Type::bigInt(),
                    ],
                    'foreignKeys' => [
                        'parent_id' => 'parent.parent_id',
                    ],
                ],
            ],
        ]);
        $parent = $storage->parent();
        $parent->createSync();
        $child = $storage->child();
        $child->createSync();
        $db = $storage->getDatabasePool()->random();
        $db->execSync('INSERT INTO "parent" DEFAULT VALUES');
        $db->execSync('INSERT INTO "parent" DEFAULT VALUES');
        $db->execSync('INSERT INTO "child" ("parent_id") VALUES (1), (2), (2)');
        //
        $promise = $db->exec('INSERT INTO "child" ("parent_id") VALUES (3)');
        Promise\wait(Promise\any([$promise]));
        $c = count($db->allSync('SELECT * FROM "child"'));
        $this->assertTrue($c === 3);
        //
        $promise = $db->exec('UPDATE "child" SET "parent_id" = 3 WHERE "parent_id" = 2');
        Promise\wait(Promise\any([$promise]));
        $c = count($db->allSync('SELECT * FROM "child" WHERE "parent_id" = 3'));
        $this->assertTrue($c === 0);
        //
        $db->execSync('UPDATE "parent" SET "parent_id" = 3 WHERE "parent_id" = 2');
        $c = count($db->allSync('SELECT * FROM "child" WHERE "parent_id" = 3'));
        $this->assertTrue($c === 2);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_1()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                        'field2' => Type::string(true),
                        'field3' => Type::int(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync();
        $this->assertTrue($id > 0);
        $id = $table->get([$id])->generator()->key();
        $this->assertTrue($id > 0);
        $this->assertTrue(iterator_to_array($table->get([])->generator()) === []);
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue(is_object($bean));
        $values = $bean->getValues();
        $this->assertEquals(['field1' => '', 'field2' => null, 'field3' => 0], $values);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id1 = $table->insertSync(['field1' => 'text1']);
        $id2 = $table->insertSync(['field1' => 'text2']);
        [$id1 => $bean1, $id2 => $bean2] = iterator_to_array($table->get([$id1, $id2])->generator());
        $values1 = $bean1->getValues();
        $values2 = $bean2->getValues();
        $this->assertTrue($values1 === ['field1' => 'text1']);
        $this->assertTrue($values2 === ['field1' => 'text2']);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_1()
    {
        $this->expectException(RuntimeException::class);
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        if (mt_rand(0, 1)) {
            $bean = $table->getDatabaseBean(1);
        } else {
            $bean = $table->getDatabaseBean();
            $bean->setId(1);
        }
        $this->assertTrue($bean instanceof Bean);
        $bean->field1 = 'foo';
        $bean->insertSync();
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_2()
    {
        $this->expectException(RuntimeException::class);
        $storage = \Container\getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $bean = $table->getDatabaseBean();
        $this->assertTrue($bean instanceof Bean);
        $bean->field3 = 'foo';
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_3()
    {
        $this->expectException(RuntimeException::class);
        $storage = \Container\getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $bean = $table->getDatabaseBean();
        $bean->field1 = 'foo';
        $bean->update((bool) mt_rand(0, 1));
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_4()
    {
        $this->expectException(RuntimeException::class);
        $storage = \Container\getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $bean = $table->getDatabaseBean();
        $bean->delete();
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_5()
    {
        $this->expectException(RuntimeException::class);
        $storage = \Container\getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $table->insertSync(['foo' => 'bar']);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_update()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                        'field2' => Type::int(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync();
        $bean = $table->get([$id])->generator()->current();
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
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === '1');
        $this->assertTrue($bean->field2 === 10);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_delete()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync(['field1' => 'foo']);
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'foo');
        $this->assertTrue($bean->deleteSync());
        $this->assertFalse($bean->deleteSync());
        $_ = iterator_to_array($table->get([$id])->generator());
        $this->assertTrue($_ === []);
        $id = $table->insertSync();
        $this->assertTrue($table->deleteSync([$id]) === $storage->getDatabasePool()->size());
    }

    /**
     * @test
     */
    public function test_case_storage_crud_insert()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $bean = $table->getDatabaseBean();
        $bean->field1 = 'foo';
        $id = $bean->insertSync();
        $this->assertTrue($id > 0);
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'foo');
    }

    /**
     * @test
     */
    public function test_case_storage_json()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'json' => Type::json(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $values = [
            'string',
            ['string'],
            ['foo' => 'bar'],
        ];
        foreach ($values as $value) {
            $id = $table->insertSync(['json' => $value]);
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->json) === serialize($value));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_binary()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'binary' => Type::binary(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $values = [
            '',
            chr(0),
            str_repeat(chr(1), 1E7) . str_repeat(chr(2), 1E7),
        ];
        foreach ($values as $value) {
            $id = $table->insertSync(['binary' => $value]);
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->binary) === serialize($value));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_string_array()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'array' => Type::stringArray(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
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
            $id = $table->insertSync(['array' => $value0]);
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->array) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_int_array()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'array' => Type::intArray(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
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
            $id = $table->insertSync(['array' => $value0]);
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->array) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_enum()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'enum' => Type::enum(['foo', 'bar']),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync();
        $bean = $table->get([$id])->generator()->current();
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
            $id = $table->insertSync(['enum' => $value0]);
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->enum) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_1()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ] + Storage::getPrimarySecondaryClusterConfig(0),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $table->insertSync();
        $pool = $table->getDatabasePool();
        $names = $pool->names();
        $tbl = $table->getDatabaseTable();
        $fields = $pool->instance($names[0])->fieldsSync($tbl);
        $this->assertTrue(count($fields) === 2);
        $fields = $pool->instance($names[1])->fieldsSync($tbl);
        $this->assertTrue(count($fields) === 1);
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ] + Storage::getShardsClusterConfig(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        foreach (range(1, 1000) as $_) {
            $table->insertSync();
        }
        $count = count($names = $table->getDatabasePool()->names());
        $diff = 0.3 * 1000 / $count;
        $min = 1000 / $count - $diff;
        $max = 1000 / $count + $diff;
        foreach ($names as $name) {
            $db = $table->getDatabasePool()->instance($name);
            $c = $db->countSync($table->getDatabaseTable());
            $this->assertTrue($min <= $c && $c <= $max);
        }
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_3()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ] + Storage::getShardsClusterConfig('field1'),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        foreach (range(1, 100) as $_) {
            $ids[] = $table->insertSync(['field1' => mt_rand()]);
        }
        $ids = array_flip($ids);
        $id1 = array_rand($ids);
        $field1 = $table->get([$id1])->generator()->current()->field1;
        $id2 = $table->insertSync(['field1' => $field1]);
        $size = $table->getDatabasePool()->size();
        $this->assertTrue($id1 % $size === $id2 % $size);
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_4()
    {
        $config = [
            'table' => [
                'database' => [
                    'fields' => [
                        'field1' => Type::string(),
                    ],
                ] + Storage::getShardsClusterConfig(),
            ],
        ];
        $storage = \Container\getStorage($config);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        foreach (range(1, 10) as $_) {
            $ids[] = $table->insertSync(['field1' => 'foo']);
        }
        $id = $ids[array_rand($ids)];
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'foo');
        $bean->field1 = 'bar';
        $bean->updateSync();
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'bar');
        $bean->deleteSync();
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean === null);
    }

    /**
     * @test
     */
    public function test_case_storage_iterate_1()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ] + (mt_rand(0, 1) ? Storage::getShardsClusterConfig() : []),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        [$first, $last] = [0, 2000];
        foreach (range($first, $last) as $_) {
            $ids[] = $table->insertSync();
        }
        sort($ids);
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 10);
        $params = ['config' => compact('iterator_chunk_size')];
        foreach ($table->iterate($params)->generator() as $bean) {
            $_ids[] = $bean->getId();
        }
        sort($_ids);
        $this->assertEquals($ids, $_ids);
        arsort($ids);
        $ids = array_values($ids);
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 10);
        $params = ['config' => compact('iterator_chunk_size'), 'asc' => false];
        foreach ($table->iterate($params)->generator() as $bean) {
            $_ids[] = $bean->getId();
        }
        arsort($_ids);
        $_ids = array_values($_ids);
        $ids = array_values($ids);
        $this->assertEquals($ids, $_ids);
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 10);
        $params = ['config' => compact('iterator_chunk_size'), 'rand' => true];
        foreach ($table->iterate($params)->generator() as $bean) {
            $_ids[] = $bean->getId();
            $this->assertTrue(!empty($bean->getValues()));
            $this->assertTrue(!isset($bean->getValues()[$table->getDatabasePk()]));
        }
        $this->assertTrue($_ids[0] !== 1);
        sort($ids);
        sort($_ids);
        $this->assertEquals($ids, $_ids);
        foreach ($table->iterate($params)->generator() as $id => $values) {
            $this->assertTrue($id > 0);
            $this->assertTrue($values->text1 === '');
        }
    }

    /**
     * @test
     */
    public function test_case_storage_filter()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ] + Storage::getShardsClusterConfig(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        foreach (range(1, 1000) as $_) {
            $table->insertSync(['text1' => mt_rand(1, 3)]);
        }
        $this->assertTrue(
            count(iterator_to_array($table->filter(['text1' => 1])->generator())) +
            count(iterator_to_array($table->filter(['text1' => 2])->generator())) +
            count(iterator_to_array($table->filter(['text1' => 3])->generator())) ===
            1000
        );
        $this->assertTrue($table->existsSync(['text1' => 1]));
        $this->assertTrue(!$table->existsSync(['text1' => 10]));
    }

    /**
     * @test
     */
    public function test_case_storage_reid()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync(['text1' => 'foo']);
        $this->assertTrue($table->reidSync($id, $id + 1));
        $null = $table->get([$id])->generator()->current();
        $this->assertTrue($null === null);
        $bean = $table->get([$id + 1])->generator()->current();
        $this->assertTrue($bean->text1 === 'foo');
    }

    /**
     * @test
     */
    public function test_case_storage_sort_chains()
    {
        $storage = \Container\getStorage([
            'table' => [],
        ]);
        $table = $storage->table();
        $result = $this->call($table, 'getSortChains', [1 => 1, 2 => 2]);
        $this->assertTrue($result[0] === [2, 1]);
        $result = $this->call($table, 'getSortChains', [1 => 1, 2 => 3, 3 => 2]);
        $this->assertTrue($result[0] === [3, 2, 1]);
        $result = $this->call($table, 'getSortChains', [1 => 1, 2 => 3, 3 => 2, 10 => 10, 11 => 11]);
        $this->assertTrue($result[0] === [11, 1]);
        $this->assertTrue($result[1] === [3, 10, 2]);
    }

    /**
     * @test
     */
    public function test_case_storage_table_sort()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'text1' => Type::int(),
                    ],
                    'getSortScore' => function ($bean) {
                        return $bean->getValues()['text1'];
                    },
                ] + (mt_rand(0, 1) ? Storage::getShardsClusterConfig() : []),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $text1s = [];
        foreach (range(1, array_rand(array_flip([10, 100, 1000]))) as $_) {
            $_ = mt_rand(1, 100);
            $text1s[] = $_;
            $table->insertSync(['text1' => $_]);
        }
        $this->assertTrue($table->sort());
        $min = min($text1s);
        $max = max($text1s);
        $this->assertTrue($min < $max);
        $collect = [[], []];
        foreach ($storage->getDatabasePool()->names() as $name) {
            $gen = $table->iterate(['asc' => true, 'poolFilter' => $name])->generator();
            $collect[0][] = $gen->current()->text1;
            $gen = $table->iterate(['asc' => false, 'poolFilter' => $name])->generator();
            $collect[1][] = $gen->current()->text1;
        }
        $this->assertEquals(min($collect[1]), $min);
        $this->assertEquals(max($collect[0]), $max);
    }

    /**
     * @test
     */
    public function test_case_storage_cache_1()
    {
        $cacheTtl = 3;
        $tbl = 'table' . mt_rand();
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'table' => $tbl,
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ],
                'cache' => [
                    'ttl' => $cacheTtl,
                ],
            ],
        ]);
        $table = $storage->table();
        $pk = $table->getDatabasePk();
        $table->createSync();
        $id = $table->insertSync(['text1' => 'foo']);
        $text1 = $table->get([$id])->generator()->current()->text1;
        $this->assertTrue($text1 === 'foo');
        $table->getDatabasePool()->execSync("UPDATE {$tbl} SET text1 = ? WHERE {$pk} = ?", 'bar', $id);
        $text1 = $table->get([$id])->generator()->current()->text1;
        $this->assertTrue($text1 === 'foo');
        sleep($cacheTtl - 1);
        $text1 = $table->get([$id])->generator()->current()->text1;
        $this->assertTrue($text1 === 'foo');
        sleep(2);
        $text1 = $table->get([$id])->generator()->current()->text1;
        $this->assertTrue($text1 === 'bar');
    }

    /**
     * @test
     */
    public function test_case_storage_cache_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ],
                'cache' => [],
            ],
        ]);
        $table = $storage->table();
        $pk = $table->getDatabasePk();
        $table->createSync();
        $id = $table->insertSync(['text1' => 'foo']);
        $bean = $table->get([$id])->generator()->current();
        $bean->text1 = 'bar';
        $bean->updateSync();
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->text1 === 'bar');
    }

    /**
     * @test
     */
    public function test_case_storage_cache_3()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ],
                'cache' => [],
            ],
        ]);
        $table = $storage->table();
        $pk = $table->getDatabasePk();
        $table->createSync();
        $id = $table->insertSync(['text1' => 'foo']);
        $bean = $table->get([$id])->generator()->current();
        $bean->deleteSync();
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean === null);
    }

    /**
     * @test
     */
    public function test_case_storage_cache_4()
    {
        $cacheTtl = 3;
        $tbl = 'table' . mt_rand();
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'table' => $tbl,
                    'fields' => [
                        'text1' => Type::string(),
                    ],
                ],
                'cache' => [
                    'ttl' => $cacheTtl,
                    'fieldsToId' => ['text1'],
                ],
            ],
        ]);
        $table = $storage->table();
        $pk = $table->getDatabasePk();
        $table->createSync();
        $id = $table->insertSync(['text1' => 'foo1']);
        $text1 = $table->get([$id])->generator()->current()->text1;
        $this->assertTrue($text1 === 'foo1');
        $text1 = $table->filter(['text1' => 'foo1'])->generator()->current()->text1;
        $this->assertTrue($text1 === 'foo1');
        $sql = "UPDATE {$tbl} SET text1 = ? WHERE {$pk} = ?";
        $table->getDatabasePool()->execSync($sql, 'bar1', $id);
        $text1 = $table->filter(['text1' => 'foo1'])->generator()->current()->text1;
        $this->assertTrue($text1 === 'foo1');
        sleep($cacheTtl + 1);
        $this->assertTrue($table->filter(['text1' => 'foo1'])->generator()->current() === null);
    }

    /**
     * @test
     */
    public function test_case_storage_bitmap_1()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'boolean' => Type::bool(),
                    ],
                ],
                'bitmap' => [
                    'fields' => [
                        'boolean' => Type::bitmapBool(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->addSync(1E6);
        $this->assertTrue($id === intval(1E6));
        $list = $table->getBitmapPool()->listSync();
        $this->assertTrue(in_array($table->getBitmapIndex(), current($list)));
        foreach (range(1, 100) as $_) {
            $this->assertTrue($table->insertSync(['boolean' => mt_rand(0, 1)]) > 0);
        }
        $table->bitmapPopulate();
        $bitmap = $table->getBitmapPool()->random();
        $ids = [];
        foreach ($bitmap->search($table, '*')->generator() as $id => $bean) {
            $ids[] = $id;
            $this->assertTrue($id > 0);
            $this->assertTrue(is_object($bean));
        }
        $this->assertTrue($ids === range(1, 100));
        $this->assertTrue($bitmap->search($table, '*')->getSize() === 100);
        $_0 = $bitmap->search($table, '@boolean:0')->getSize();
        $_1 = $bitmap->search($table, '@boolean:1')->getSize();
        $this->assertTrue($_0 + $_1 === 100);
        $this->assertTrue($_0 > 10 && $_1 > 10);
    }

    /**
     * @test
     */
    public function test_case_storage_bitmap_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'boolean' => Type::bool(),
                    ],
                ] + Storage::getShardsClusterConfig(),
                'bitmap' => [
                    'fields' => [
                        'boolean' => Type::bitmapBool(),
                    ],
                ] + Storage::getShardsClusterConfig(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        foreach (range(1, 100) as $_) {
            $table->insertSync(['boolean' => mt_rand(0, 1)]);
        }
        $table->bitmapPopulate();
        $this->assertTrue($table->search('*')->getSize() === 100);
        $_0 = $table->search('@boolean:0')->getSize();
        $_1 = $table->search('@boolean:1')->getSize();
        $this->assertTrue($_0 + $_1 === 100);
        $this->assertTrue($_0 > 10 && $_1 > 10);
        foreach ($table->search('*')->generator() as $id => $bean) {
            $ex = $ex ?? $id;
            $this->assertTrue($ex <= $id);
            $ex = $id;
        }
    }

    /**
     * @test
     */
    public function test_case_storage_bitmap_3()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'boolean' => Type::bool(),
                        'int' => Type::int(),
                    ],
                    'getSortScore' => function ($bean) {
                        return $bean->int;
                    },
                ] + Storage::getShardsClusterConfig(),
                'bitmap' => [
                    'fields' => [
                        'boolean' => Type::bitmapBool(),
                    ],
                ] + Storage::getShardsClusterConfig(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        foreach (range(1, 100) as $_) {
            $table->insertSync([
                'boolean' => mt_rand(0, 1),
                'int' => mt_rand(1, 100),
            ]);
        }
        $table->sort();
        $table->bitmapPopulate();
        $query = mt_rand(0, 1) ? '*' : '@boolean:' . mt_rand(0, 1);
        foreach ($table->search($query)->generator() as $id => $bean) {
            $ex = $ex ?? $bean->int;
            $this->assertTrue($bean->int <= $ex, "{$bean->int} <= {$ex}");
            $ex = $bean->int;
        }
    }

    /**
     * @test
     */
    public function test_case_storage_bitmap_4()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'boolean' => Type::bool(),
                    ],
                ],
                'bitmap' => [
                    'fields' => [
                        'boolean' => Type::bitmapBool(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        foreach (range(1, 100) as $_) {
            $table->insertSync(['boolean' => mt_rand(0, 1)]);
        }
        $table->bitmapPopulate();
        $iterator = $table->getBitmapPool()->random()->search($table, '*');
        @ $pointer = (int) $iterator->getSearchIteratorState()['pointer'];
        $this->assertTrue($pointer === 0);
        $iterator->advanceSync();
        @ $pointer = (int) $iterator->getSearchIteratorState()['pointer'];
        $this->assertTrue($pointer > 0);
        $iterator = $table->search('*');
        $pointers = array_column($iterator->getSearchIteratorState(), 'pointer');
        $this->assertTrue(array_sum($pointers) === 0);
        $iterator->advanceSync();
        $pointers = array_column($iterator->getSearchIteratorState(), 'pointer');
        $this->assertTrue(array_sum($pointers) > 0);
    }

    /**
     * @test
     */
    public function test_case_storage_bitmap_5()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'boolean' => Type::bool(),
                        'int' => Type::int(),
                    ],
                    'getSortScore' => function ($bean) {
                        return $bean->int;
                    },
                ] + Storage::getShardsClusterConfig(),
                'bitmap' => [
                    'fields' => [
                        'boolean' => Type::bitmapBool(),
                    ],
                ] + Storage::getShardsClusterConfig(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $all = [];
        foreach (range(1, 2) as $_) {
            $all[] = $table->insertSync([
                'boolean' => mt_rand(0, 1),
                'int' => mt_rand(1, 100),
            ]);
        }
        sort($all);
        $table->sort();
        $table->bitmapPopulate();
        $iterator = $table->search('*');
        $ids = [];
        while ($iterator->advanceSync()) {
            [$id, $bean] = $iterator->getCurrent();
            $ids[] = $id;
            if (mt_rand(0, 1)) {
                $iterator = $table->search($iterator->getSearchIteratorState());
            }
        }
        sort($ids);
        $this->assertEquals($all, $ids);
    }

    /**
     * @test
     */
    public function test_case_storage_massive_create()
    {
        $rnd = (bool) mt_rand(0, 1);
        $bitmap = getBitmap();
        $storage = \Container\getStorage([
            'table1' => [
                'bitmap' => [],
            ],
            'table2' => [
                'bitmap' => $rnd ? [] : null,
            ],
        ]);
        $storage->createSync();
        $storage->bitmapCreate();
        $storage->bitmapPopulate();
        $storage->sort();
        $this->assertTrue(true);
        $db = $storage->getDatabasePool()->random();
        $this->assertTrue(wait($db->tableExists('table1')));
        $this->assertTrue(wait($db->tableExists('table2')));
        $this->assertTrue(in_array('table1', $bitmap->LIST()));
        $this->assertTrue($rnd xor !in_array('table2', $bitmap->LIST()));
        $storage->dropSync();
        $storage->bitmapDrop();
        $this->assertTrue(!wait($db->tableExists('table1')));
        $this->assertTrue(!wait($db->tableExists('table2')));
        $this->assertTrue(!in_array('table1', $bitmap->LIST()));
        $this->assertTrue(!in_array('table2', $bitmap->LIST()));
    }
}
