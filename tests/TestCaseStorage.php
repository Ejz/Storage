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

    /**
     * @test
     */
    public function test_case_storage_iterate_1()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'text1' => Type::string(),
                ],
            ] + (mt_rand(0, 1) ? Storage::getShardsClusterConfig() : []),
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
        $this->assertEquals($ids, $_ids);
        arsort($ids);
        $ids = array_values($ids);
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 10);
        $params = ['config' => compact('iterator_chunk_size'), 'asc' => false];
        foreach ($table->iterate($params)->generator() as $bean) {
            $_ids[] = $bean->getId();
        }
        $this->assertEquals($ids, $_ids);
        $_ids = [];
        $iterator_chunk_size = mt_rand(1, 10);
        $params = ['config' => compact('iterator_chunk_size'), 'rand' => true];
        foreach ($table->iterate($params)->generator() as $bean) {
            $_ids[] = $bean->getId();
            $this->assertTrue(!empty($bean->getValues()));
            $this->assertTrue(!isset($bean->getValues()[$table->getPk()]));
        }
        $this->assertTrue($_ids[0] !== 1);
        sort($ids);
        sort($_ids);
        $this->assertEquals($ids, $_ids);
        $params = ['returnFields' => false];
        foreach ($table->iterate($params)->generator() as $id => $values) {
            $this->assertTrue($id > 0);
            $this->assertTrue($values['text1'] === '');
        }
    }

    /**
     * @test
     */
    public function test_case_storage_filter()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'text1' => Type::string(),
                ],
            ] + Storage::getShardsClusterConfig(),
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
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'text1' => Type::string(),
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
        $storage = getStorage([
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
}
