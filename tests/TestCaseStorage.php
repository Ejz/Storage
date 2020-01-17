<?php

namespace Tests;

use Ejz\Type;
use Ejz\Bean;
use Ejz\Storage;
use Ejz\Index;
use RuntimeException;

use function Amp\Promise\wait;
use function Container\getStorage;

class TestCaseStorage extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_storage_create_1()
    {
        $storage = getStorage([
            'table' => [
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
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->table();
        wait($table->create());
        $db = $table->getPool()->random();
        $fields = wait($db->fields('table'));
        $_fields = array_keys($_fields);
        $_fields[] = $table->getPk();
        sort($fields);
        sort($_fields);
        $this->assertEquals($_fields, $fields);
    }

    /**
     * @test
     */
    public function test_case_storage_create_2()
    {
        $storage = getStorage([
            'table' => [
                'table' => 'foo',
            ],
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->table();
        wait($table->create());
        $db = $table->getPool()->random();
        $this->assertTrue(wait($db->tableExists('foo')));
        $this->assertTrue(!wait($db->tableExists('table')));
    }

    /**
     * @test
     */
    public function test_case_storage_create_3()
    {
        $storage = getStorage([
            'Table' => [
                'pk' => 'foo',
            ],
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->Table();
        wait($table->create());
        $db = $table->getPool()->random();
        $fields = wait($db->fields('table'));
        $this->assertTrue($fields === ['foo']);
    }

    /**
     * @test
     */
    public function test_case_storage_create_4()
    {
        $storage = getStorage([
            'Table' => [
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
        $table = $storage->Table();
        wait($table->create());
        $db = $table->getPool()->random();
        $indexes = wait($db->indexes('table'));
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
        $storage = getStorage([
            'Table' => [
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
        $table = $storage->Table();
        wait($table->create());
        $db = $table->getPool()->random();
        $indexes = wait($db->indexes('t'));
        $indexes = array_map(function ($fields) {
            return implode(',', $fields);
        }, $indexes);
        $indexes = array_flip($indexes);
        $this->assertTrue(isset($indexes['f1']));
        wait($db->exec('INSERT INTO t (f1) VALUES (1)'));
        wait(\Amp\Promise\any([$db->exec('INSERT INTO t (f1) VALUES (1)')]));
        $c = count(wait($db->all('SELECT * FROM t')));
        $this->assertTrue($c === 1);
    }

    /**
     * @test
     */
    public function test_case_storage_create_6()
    {
        $storage = getStorage([
            'parent' => [
                'pk' => 'parent_id',
                'fields' => [
                    'text' => Type::string(true),
                ],
            ],
            'child' => [
                'pk' => 'id',
                'fields' => [
                    'parent_id' => Type::bigInt(),
                ],
                'foreignKeys' => [
                    'parent_id' => 'parent.parent_id',
                ],
            ],
        ]);
        $parent = $storage->parent();
        wait($parent->create());
        $child = $storage->child();
        wait($child->create());
        $db = $storage->getPool()->random();
        wait($db->exec('INSERT INTO "parent" DEFAULT VALUES'));
        wait($db->exec('INSERT INTO "parent" DEFAULT VALUES'));
        wait($db->exec('INSERT INTO "child" ("parent_id") VALUES (1), (2), (2)'));
        //
        wait(\Amp\Promise\any([$db->exec('INSERT INTO "child" ("parent_id") VALUES (3)')]));
        $c = count(wait($db->all('SELECT * FROM "child"')));
        $this->assertTrue($c === 3);
        //
        wait(\Amp\Promise\any([$db->exec('UPDATE "child" SET "parent_id" = 3 WHERE "parent_id" = 2')]));
        $c = count(wait($db->all('SELECT * FROM "child" WHERE "parent_id" = 3')));
        $this->assertTrue($c === 0);
        //
        wait($db->exec('UPDATE "parent" SET "parent_id" = 3 WHERE "parent_id" = 2'));
        $c = count(wait($db->all('SELECT * FROM "child" WHERE "parent_id" = 3')));
        $this->assertTrue($c === 2);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_1()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                    'field2' => Type::string(true),
                    'field3' => Type::int(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $id = wait($table->insert());
        $this->assertTrue($id > 0);
        [$id => $bean] = iterator_to_array($table->get([$id])->generator());
        $values = $bean->getValues();
        $this->assertEquals(['field1' => '', 'field2' => null, 'field3' => 0], $values);
    }

    /**
     * @test
     */
    public function test_case_storage_crud_2()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $id1 = wait($table->insert(['field1' => 'text1']));
        $id2 = wait($table->insert(['field1' => 'text2']));
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
        $storage = getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        if (mt_rand(0, 1)) {
            $bean = $table->getBean(1);
        } else {
            $bean = $table->getBean();
            $bean->setId(1);
        }
        $this->assertTrue($bean instanceof Bean);
        $bean->field1 = 'foo';
        wait($bean->insert());
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_2()
    {
        $this->expectException(RuntimeException::class);
        $storage = getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $bean = $table->getBean();
        $bean->field3 = 'foo';
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_3()
    {
        $this->expectException(RuntimeException::class);
        $storage = getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $bean = $table->getBean();
        $bean->field1 = 'foo';
        $bean->update((bool) mt_rand(0, 1));
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_4()
    {
        $this->expectException(RuntimeException::class);
        $storage = getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $bean = $table->getBean();
        $bean->delete();
    }

    /**
     * @test
     */
    public function test_case_storage_crud_exceptions_5()
    {
        $this->expectException(RuntimeException::class);
        $storage = getStorage([
            'table' => [
                'field1' => Type::string(),
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        wait($table->insert(['foo' => 'bar']));
    }

    /**
     * @test
     */
    public function test_case_storage_crud_update()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                    'field2' => Type::int(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $id = wait($table->insert());
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === '');
        $this->assertTrue($bean->field2 === 0);
        $bean->field1 = '1';
        $bean->field2 = '10';
        $this->assertTrue($bean->field1 === '1');
        $this->assertTrue($bean->field2 === 10);
        $this->assertTrue($bean->getValues()['field1'] === '1');
        $this->assertTrue($bean->getValues()['field2'] === 10);
        $this->assertTrue(wait($bean->update()));
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
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $id = wait($table->insert(['field1' => 'foo']));
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'foo');
        $this->assertTrue(wait($bean->delete()));
        $this->assertFalse(wait($bean->delete()));
        $_ = iterator_to_array($table->get([$id])->generator());
        $this->assertTrue($_ === []);
        $id = wait($table->insert());
        $this->assertTrue(wait($table->delete([$id])) === $storage->getPool()->size());
    }

    /**
     * @test
     */
    public function test_case_storage_crud_insert()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $bean = $table->getBean();
        $bean->field1 = 'foo';
        $id = wait($bean->insert());
        $this->assertTrue($id > 0);
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'foo');
    }

    /**
     * @test
     */
    public function test_case_storage_json()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'json' => Type::json(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $values = [
            'string',
            ['string'],
            ['foo' => 'bar'],
        ];
        foreach ($values as $value) {
            $id = wait($table->insert(['json' => $value]));
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->json) === serialize($value));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_binary()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'binary' => Type::binary(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $values = [
            '',
            chr(0),
            str_repeat(chr(1), 1E7) . str_repeat(chr(2), 1E7),
        ];
        foreach ($values as $value) {
            $id = wait($table->insert(['binary' => $value]));
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->binary) === serialize($value));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_string_array()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'array' => Type::stringArray(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
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
            $id = wait($table->insert(['array' => $value0]));
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->array) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_int_array()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'array' => Type::intArray(),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
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
            $id = wait($table->insert(['array' => $value0]));
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->array) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_enum()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'enum' => Type::enum(['foo', 'bar']),
                ],
            ],
        ]);
        $table = $storage->table();
        wait($table->create());
        $id = wait($table->insert());
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
            $id = wait($table->insert(['enum' => $value0]));
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue(serialize($bean->enum) === serialize($value1));
        }
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_1()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ] + Storage::getPrimarySecondaryClusterConfig(0),
        ]);
        $table = $storage->table();
        wait($table->create());
        wait($table->insert());
        $pool = $table->getPool();
        $names = $pool->names();
        $fields = wait($pool->db($names[0])->fields($table->getTable()));
        $this->assertTrue(count($fields) === 2);
        $fields = wait($pool->db($names[1])->fields($table->getTable()));
        $this->assertTrue(count($fields) === 1);
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_2()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ] + Storage::getShardsClusterConfig(),
        ]);
        $table = $storage->table();
        wait($table->create());
        foreach (range(1, 1000) as $_) {
            wait($table->insert());
        }
        $count = count($names = $table->getPool()->names());
        $diff = 0.3 * 1000 / $count;
        $min = 1000 / $count - $diff;
        $max = 1000 / $count + $diff;
        foreach ($names as $name) {
            $db = $table->getPool()->db($name);
            $c = wait($db->count($table->getTable()));
            $this->assertTrue($min <= $c && $c <= $max);
        }
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_3()
    {
        $names = $this->pool->names();
        $table = 'table' . mt_rand();
        $config = [
            $table => [
                'table' => $table,
                'fields' => [
                    'field1' => Type::string(),
                ],
            ] + Storage::getShardsClusterConfig('field1'),
        ];
        foreach ($names as $name) {
            $config[$name] = [
                'table' => $table,
                'fields' => [
                    'field1' => Type::string(),
                ],
                'getWritablePool' => function () use ($name) {
                    return [$name];
                },
                'getReadablePool' => function () use ($name) {
                    return [$name];
                },
            ];
        }
        $storage = getStorage($config);
        $storageTable = $storage->$table();
        wait($storageTable->create());
        $ids = [];
        foreach (range(1, 1000) as $_) {
            $ids[] = wait($storageTable->insert(['field1' => mt_rand()]));
        }
        $count = count($names = $storageTable->getPool()->names());
        $diff = 0.3 * 1000 / $count;
        $min = 1000 / $count - $diff;
        $max = 1000 / $count + $diff;
        foreach ($names as $name) {
            $db = $storageTable->getPool()->db($name);
            $c = wait($db->count($storageTable->getTable()));
            $this->assertTrue($min <= $c && $c <= $max);
        }
        $id = $ids[array_rand($ids)];
        $field1 = $storageTable->get([$id])->generator()->current()->field1;
        $this->assertTrue(!empty($field1));
        $name = $this->call($storageTable, 'getReadablePool', $id, null)->random()->getName();
        $this->assertTrue(!empty($name));
        $f = $storage->$name()->get([$id])->generator()->current()->field1;
        $this->assertTrue($field1 === $f);
    }

    /**
     * @test
     */
    public function test_case_storage_cluster_4()
    {
        $config = [
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ] + Storage::getShardsClusterConfig(),
        ];
        $storage = getStorage($config);
        $table = $storage->table();
        wait($table->create());
        $ids = [];
        foreach (range(1, 10) as $_) {
            $ids[] = wait($table->insert(['field1' => 'foo']));
        }
        $id = $ids[array_rand($ids)];
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'foo');
        $bean->field1 = 'bar';
        wait($bean->update());
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean->field1 === 'bar');
        wait($bean->delete());
        $bean = $table->get([$id])->generator()->current();
        $this->assertTrue($bean === null);
    }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_filter()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text1' => [
    //                 ],
    //                 'text2' => [
    //                 ],
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     foreach (range(1, 1000) as $_) {
    //         $table->insert($_ = ['text1' => mt_rand() % 5, 'text2' => mt_rand() % 5]);
    //     }
    //     $elems = $table->filter(['text1' => 1]);
    //     $this->assertTrue(100 < count($elems) && count($elems) < 300);
    //     $this->assertTrue(
    //         count($table->filter(['text1' => 0])) +
    //         count($table->filter(['text1' => 1])) +
    //         count($table->filter(['text1' => 2])) +
    //         count($table->filter(['text1' => 3])) +
    //         count($table->filter(['text1' => 4])) ===
    //         1000
    //     );
    //     [$id, $row] = [key($elems), current($elems)];
    //     $this->assertTrue($id > 0);
    //     $this->assertTrue(isset($row['text1']));
    //     $this->assertTrue(isset($row['text2']));
    //     $this->assertTrue(!isset($row['table_id']));
    //     $elem = current($table->get($id, 'text2'));
    //     $this->assertTrue(!isset($elem['text1']));
    //     $this->assertTrue(isset($elem['text2']));
    //     $elem = current($table->get($id));
    //     $this->assertTrue(isset($elem['text1']));
    //     $this->assertTrue(isset($elem['text2']));
    //     $elem = current($table->get($id, []));
    //     $this->assertTrue(!isset($elem['text1']));
    //     $this->assertTrue(!isset($elem['text2']));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_iterate_1()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text1' => [
    //                 ],
    //                 'text2' => [
    //                 ],
    //             ],
    //             'modifiers' => [
    //                 '~^text3$~i' => function ($match, $value) {
    //                     return [
    //                         'text2',
    //                         ['get_pattern' => 'CONCAT(%s, \'hello\')'],
    //                         $value,
    //                     ];
    //                 },
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     foreach (range(1, 2E3) as $_) {
    //         $table->insert($_ = ['text1' => mt_rand() % 5, 'text2' => mt_rand() % 5]);
    //     }
    //     $ids = [];
    //     foreach ($table->iterate(null, 'text3') as $id => $row) {
    //         $ids[] = $id;
    //     }
    //     $this->assertTrue(isset($row['text3']));
    //     $this->assertTrue(!!preg_match('~^\dhello$~', $row['text3']));
    //     $this->assertTrue(min($ids) === 1);
    //     $this->assertTrue(max($ids) === 2000);
    //     $this->assertTrue(count($ids) === 2000);
    //     $all = array_fill_keys($ids, true);
    //     $ids = [];
    //     foreach ($table->iterate(null, null, ['rand' => true]) as $id => $row) {
    //         $ids[] = $id;
    //         unset($all[$id]);
    //     }
    //     $this->assertTrue($ids[0] !== 1 && $ids[1999] !== 2000);
    //     $this->assertTrue(!$all);
    //     $this->assertTrue(min($ids) === 1);
    //     $this->assertTrue(max($ids) === 2000);
    //     $this->assertTrue(count($ids) === 2000);
    //     $this->assertTrue(count(array_unique($ids)) === 2000);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_iterate_2()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text1' => [],
    //             ],
    //             'get_shards_by_id' => function ($id, $shards) {
    //                 $c = count($shards);
    //                 $keys = array_keys($shards);
    //                 $id %= $c;
    //                 $id -= 1;
    //                 $id = $id < 0 ? $id + $c : $id;
    //                 $key = $keys[$id];
    //                 return [$shards[$key]];
    //             },
    //             'get_write_shards_by_values' => function ($values, $shards) {
    //                 return [$shards[array_rand($shards)]];
    //             },
    //             'primary_key_start_with' => function ($shard, $shards) {
    //                 $index = (int) array_search($shard, $shards);
    //                 return 1 + $index;
    //             },
    //             'primary_key_increment_by' => function ($shard, $shards) {
    //                 return count($shards);
    //             },
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     foreach (range(1, 2E3) as $_) {
    //         $table->insert($_ = ['text1' => mt_rand() % 5]);
    //     }
    //     $ids = [];
    //     foreach ($table->iterate() as $id => $row) {
    //         $ids[] = $id;
    //     }
    //     $this->assertTrue(min($ids) === 1);
    //     $this->assertTrue(100 * abs(max($ids) - 2E3) / 2E3 < 10);
    //     $this->assertTrue(count($ids) === 2000);
    //     $this->assertTrue(count(array_unique($ids)) === 2000);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_update()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text1' => [],
    //             ],
    //             'get_shards_by_id' => function ($id, $shards) {
    //                 $c = count($shards);
    //                 $keys = array_keys($shards);
    //                 $id %= $c;
    //                 $id -= 1;
    //                 $id = $id < 0 ? $id + $c : $id;
    //                 $key = $keys[$id];
    //                 return [$shards[$key]];
    //             },
    //             'get_write_shards_by_values' => function ($values, $shards) {
    //                 return [$shards[array_rand($shards)]];
    //             },
    //             'primary_key_start_with' => function ($shard, $shards) {
    //                 $index = (int) array_search($shard, $shards);
    //                 return 1 + $index;
    //             },
    //             'primary_key_increment_by' => function ($shard, $shards) {
    //                 return count($shards);
    //             },
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $id = $table->insert(['text1' => 'foo']);
    //     $ok = $table->update($id, ['text1' => 'bar']);
    //     $this->assertTrue($ok);
    //     $this->assertTrue(current($table->get($id))['text1'] === 'bar');
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_modifiers_1()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text1' => [],
    //                 'json1' => [
    //                     'type' => TableDefinition::TYPE_JSON,
    //                 ],
    //             ],
    //             'modifiers' => [
    //                 '~^(.*)\|trim$~i' => function ($match, $value) {
    //                     $append = ['set' => function ($v) { return trim($v); }];
    //                     return [$match[1], $append, $value];
    //                 },
    //                 '~^(.*)\{\}$~i' => function ($match, $value) {
    //                     $append = [
    //                         'set_pattern' => '%s || ?',
    //                     ];
    //                     return [$match[1], $append, $value];
    //                 },
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $id = $table->insert(['text1|Trim' => ' foo ']);
    //     $this->assertTrue(current($table->get($id))['text1'] === 'foo');
    //     $id = $table->insert(['json1' => ['a' => 1, 'b' => 2]]);
    //     $this->assertTrue(current($table->get($id))['json1']['a'] === 1);
    //     $table->update($id, ['json1{}' => ['c' => 3]]);
    //     $this->assertTrue(current($table->get($id))['json1']['a'] === 1);
    //     $this->assertTrue(current($table->get($id))['json1']['c'] === 3);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_modifiers_2()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'json1' => [
    //                     'type' => TableDefinition::TYPE_JSON,
    //                     'get' => function ($v) {
    //                         return ['x' => 1] + $v;
    //                     },
    //                     'set' => function ($v) {
    //                         return ['y' => 1] + $v;
    //                     },
    //                 ],
    //             ],
    //             'modifiers' => [
    //                 '~^(.*)\|ab$~i' => function ($match, $value) {
    //                     $append = [
    //                         'get' => function ($v) {
    //                             return ['a' => 1] + $v;
    //                         },
    //                         'set' => function ($v) {
    //                             return ['b' => 1] + $v;
    //                         },
    //                     ];
    //                     $append['alias'] = 'rnd';
    //                     return [$match[1], $append, $value];
    //                 },
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $id = $table->insert(['json1' => ['m' => 1]]);
    //     $json1 = current($table->get($id))['json1'];
    //     $this->assertTrue(isset($json1['x'], $json1['y'], $json1['m']));
    //     $id = $table->insert(['json1|ab' => ['m' => 2]]);
    //     $json1 = current($table->get($id))['json1'];
    //     $this->assertTrue(isset($json1['x'], $json1['b'], $json1['m']));
    //     $this->assertTrue(!isset($json1['a']));
    //     $elem = current($table->get($id, 'json1|ab'));
    //     $this->assertTrue(isset($elem['rnd']['a']));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_modifiers_3()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'int' => [
    //                     'type' => TableDefinition::TYPE_INT,
    //                     'set_pattern' => '? * 2',
    //                     'get_pattern' => '%s / 2',
    //                 ],
    //             ],
    //             'modifiers' => [
    //                 '~^(.*)\|m$~i' => function ($match, $value) {
    //                     $append = [
    //                         'set_pattern' => '? ^ 5',
    //                         'get_pattern' => '%s ^ 0.2',
    //                     ];
    //                     $append['alias'] = 'rnd';
    //                     return [$match[1], $append, $value];
    //                 },
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $id = $table->insert(['int' => 7]);
    //     $int = current($table->get($id))['int'];
    //     $this->assertTrue($int === 7);
    //     $id = $table->insert(['int|m' => 8]);
    //     $rnd = current($table->get($id, 'int|m'))['rnd'];
    //     $this->assertTrue($rnd == 8);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_foreign_key()
    // {
    //     $storage = $this->getStorage([
    //         'parent' => [
    //             'fields' => [
    //                 'int' => [
    //                     'type' => TableDefinition::TYPE_INT,
    //                 ],
    //             ],
    //         ],
    //         'child' => [
    //             'fields' => [
    //                 'int' => [
    //                     'type' => TableDefinition::TYPE_INT,
    //                 ],
    //                 'parent_id' => [
    //                     'type' => TableDefinition::TYPE_FOREIGN_KEY,
    //                     'references' => '"parent" ("parent_id")',
    //                 ],
    //             ],
    //         ],
    //     ]);
    //     $parent = $storage->parent();
    //     $parent->create();
    //     $pid = $parent->insert();
    //     $this->assertTrue($pid > 0);
    //     $child = $storage->child();
    //     $child->create();
    //     $cid = $child->insert(['parent_id' => $pid]);
    //     $this->assertTrue($cid > 0);
    //     $elem = $child->get($cid);
    //     $this->assertTrue(key($elem) === $cid);
    //     $this->assertTrue(current($elem)['parent_id'] === $pid);
    //     $this->assertTrue($parent->reid($pid, 70));
    //     $elem = $child->get($cid);
    //     $this->assertTrue(current($elem)['parent_id'] === 70);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_delete()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text' => [
    //                 ],
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $tid = $table->insert();
    //     $this->assertTrue($tid > 0);
    //     $this->assertTrue(!empty($table->get($tid)));
    //     $table->delete($tid);
    //     $this->assertTrue(empty($table->get($tid)));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_multiple_ids()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text' => [
    //                 ],
    //             ],
    //             'is_cacheable' => mt_rand() % 2,
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $tid1 = $table->insert();
    //     $tid2 = $table->insert();
    //     $this->assertTrue($tid1 > 0 && $tid2 > 0);
    //     $elems = $table->get($tid1, $tid2);
    //     $this->assertTrue(isset($elems[$tid1], $elems[$tid2]));
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_new_types()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'bool' => [
    //                     'type' => TableDefinition::TYPE_BOOL,
    //                 ],
    //                 'float' => [
    //                     'type' => TableDefinition::TYPE_FLOAT,
    //                 ],
    //                 'date' => [
    //                     'type' => TableDefinition::TYPE_DATE,
    //                 ],
    //                 'datetime' => [
    //                     'type' => TableDefinition::TYPE_DATETIME,
    //                 ],
    //                 'int_array' => [
    //                     'type' => TableDefinition::TYPE_INT_ARRAY,
    //                 ],
    //                 'text_array' => [
    //                     'type' => TableDefinition::TYPE_TEXT_ARRAY,
    //                 ],
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     //
    //     $bools = [
    //         [null, false, ''],
    //         [1, true, true],
    //         [0, true, false],
    //         [true, true, true],
    //         [false, true, false],
    //         ['', false, ''],
    //         ['1', true, true],
    //         ['0', true, false],
    //         ['t', true, true],
    //         ['f', true, false],
    //     ];
    //     foreach ($bools as [$v1, $ok, $v2]) {
    //         $tid = $table->insert(['bool' => $v1]);
    //         $this->assertTrue($ok ? $tid > 0 : !$tid, serialize($v1));
    //         if (!$ok) continue;
    //         $elem = current($table->get($tid));
    //         $this->assertTrue($elem['bool'] === $v2, serialize($v1));
    //     }
    //     //
    //     $floats = [
    //         [0, 0],
    //         ['0.1', 0.1],
    //         [1, 1],
    //         ['1.1', 1.1],
    //     ];
    //     $table = $storage->table();
    //     $table->create();
    //     foreach ($floats as [$v1, $v2]) {
    //         $tid = $table->insert(['float' => $v1]);
    //         $this->assertTrue($tid > 0, serialize($v1));
    //         $elem = current($table->get($tid));
    //         $this->assertTrue($elem['float'] === ((float) $v2), serialize($v1));
    //     }
    //     //
    //     $dates = [
    //         ['2012-01-01', '2012-01-01'],
    //     ];
    //     $table = $storage->table();
    //     $table->create();
    //     foreach ($dates as [$v1, $v2]) {
    //         $tid = $table->insert(['date' => $v1]);
    //         $this->assertTrue($tid > 0, serialize($v1));
    //         $elem = current($table->get($tid));
    //         $this->assertTrue($elem['date'] === $v2, serialize($v1));
    //     }
    //     //
    //     $datetimes = [
    //         ['2012-01-01', '2012-01-01 00:00:00'],
    //         [$dt = date('Y-m-d H:i:s', time()), $dt],
    //     ];
    //     $table = $storage->table();
    //     $table->create();
    //     foreach ($datetimes as [$v1, $v2]) {
    //         $tid = $table->insert(['datetime' => $v1]);
    //         $this->assertTrue($tid > 0, serialize($v1));
    //         $elem = current($table->get($tid));
    //         $this->assertTrue($elem['datetime'] === $v2, serialize($v1));
    //     }
    //     //
    //     $int_arrays = [
    //         [[1, '2'], [1, 2]],
    //         [['1', '2', '0'], [1, 2, 0]],
    //     ];
    //     $table = $storage->table();
    //     $table->create();
    //     foreach ($int_arrays as [$v1, $v2]) {
    //         $tid = $table->insert(['int_array' => $v1]);
    //         $this->assertTrue($tid > 0, serialize($v1));
    //         $elem = current($table->get($tid));
    //         $this->assertTrue($elem['int_array'] === $v2, serialize($v1));
    //     }
    //     //
    //     $text_arrays = [
    //         [[1, '2'], ['1', '2']],
    //         [['1', '2', '0'], ['1', '2', '0']],
    //         [['foo', 'bar'], ['foo', 'bar']],
    //     ];
    //     $table = $storage->table();
    //     $table->create();
    //     foreach ($text_arrays as [$v1, $v2]) {
    //         $tid = $table->insert(['text_array' => $v1]);
    //         $this->assertTrue($tid > 0, serialize($v1));
    //         $elem = current($table->get($tid));
    //         $this->assertTrue($elem['text_array'] === $v2, serialize($v1));
    //     }
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_jsonb_array_modifiers()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'int_array' => [
    //                     'type' => TableDefinition::TYPE_INT_ARRAY,
    //                 ],
    //                 'json' => [
    //                     'type' => TableDefinition::TYPE_JSON,
    //                 ],
    //             ],
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     $tid = $table->insert(['int_array' => [1]]);
    //     $elem = current($table->get($tid));
    //     $table->update($tid, ['int_array[]' => 2]);
    //     $table->update($tid, ['int_array[]' => [3, 4]]);
    //     $elem = current($table->get($tid));
    //     $this->assertTrue($elem['int_array'] === [1, 2, 3, 4]);
    //     $tid = $table->insert(['json' => ['foo' => 'bar', 'a' => 1]]);
    //     $table->update($tid, ['json{}' => ['a' => 2, 'b' => 3]]);
    //     $elem = current($table->get($tid));
    //     $this->assertTrue($elem['json'] === ['a' => 2, 'b' => 3, 'foo' => 'bar']);
    // }

    // /**
    //  * @test
    //  */
    // public function test_case_storage_bitmap()
    // {
    //     $storage = $this->getStorage([
    //         'table' => [
    //             'fields' => [
    //                 'text' => [
    //                     'type' => TableDefinition::TYPE_TEXT,
    //                 ],
    //             ],
    //             'get_all_shards' => function ($shards) {
    //                 return [$shards[0]];
    //             },
    //             'bitmap' => true,
    //         ],
    //     ]);
    //     $table = $storage->table();
    //     $table->create();
    //     foreach (range(1, 1000) as $_) {
    //         $tid = $table->insert(['text' => 'text' . (mt_rand() % 10)]);
    //     }
    //     $i = 0;
    //     foreach ($table->index('*') as $_) {
    //         $i++;
    //     }
    //     $this->assertTrue($i === 1000);
    //     $tables = iterator_to_array($table->index('*'));
    //     $this->assertTrue(isset($tables[1]));
    //     $this->assertTrue(isset($tables[1000]));
    // }
}
