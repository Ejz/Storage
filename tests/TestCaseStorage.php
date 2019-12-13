<?php

namespace Tests;

use Throwable;
use Ejz\Storage;
use Ejz\TableDefinition;

class TestCaseStorage extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_storage_common()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'int' => [
                        'type' => TableDefinition::TYPE_INT,
                        'index' => 'asd',
                    ],
                ],
                'is_cacheable' => mt_rand() % 2,
            ],
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->table();
        $table->create();
        foreach ($storage->getPool() as $db) {
            $fields = $db->fields('table');
            $this->assertTrue(count($fields) === 2);
            $this->assertTrue(isset($fields['int']));
            $this->assertTrue($fields['int']['type'] === 'integer');
            $this->assertTrue($fields['table_id']['type'] === 'bigint');
            $this->assertTrue($fields['table_id']['is_primary']);
        }
        $id1 = $table->insert();
        $this->assertEquals(1, $id1);
        $id2 = $table->insert();
        $this->assertEquals(2, $id2);
        $id3 = $table->insert(['int' => 0]);
        $this->assertEquals(3, $id3);
        $elem = current($table->get($id3));
        $this->assertTrue($elem['int'] === 0);
    }

    /**
     * @test
     */
    public function test_case_storage_get_set()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'blob' => [
                        'type' => TableDefinition::TYPE_BLOB,
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id1 = $table->insert(['blob' => chr(0)]);
        $elem = current($table->get($id1));
        $this->assertTrue($elem['blob'] === chr(0));
        $id2 = $table->insert(['blob' => str_repeat(chr(1), 1E7)]);
        $elem = current($table->get($id2));
        $this->assertTrue(strlen($elem['blob']) == 1E7);
        $id3 = $table->insert(['blob' => '']);
        $elem = current($table->get($id3));
        $this->assertTrue($elem['blob'] === '');
        $id4 = $table->insert();
        $elem = current($table->get($id4));
        $this->assertTrue($elem['blob'] === '');
    }

    /**
     * @test
     */
    public function test_case_storage_text_and_json()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text' => [
                    ],
                    'json' => [
                        'type' => TableDefinition::TYPE_JSON,
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id1 = $table->insert(['text' => 'hello', 'json' => ['hi']]);
        $elem = current($table->get($id1));
        $this->assertTrue($elem['text'] === 'hello');
        $this->assertTrue($elem['json'] === ['hi']);
    }

    /**
     * @test
     */
    public function test_case_storage_unique()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'u1' => [
                        'unique' => 'u1',
                    ],
                    'u2' => [
                        'unique' => 'u23',
                    ],
                    'u3' => [
                        'unique' => ['u23'],
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id1 = $table->insert(['u1' => 'foo', 'u2' => mt_rand(), 'u3' => mt_rand()]);
        $this->assertTrue($id1 === 1);
        $id2 = $table->insert(['u1' => 'foo', 'u2' => mt_rand(), 'u3' => mt_rand()]);
        $this->assertTrue($id2 === 0);
        $id3 = $table->insert(['u1' => mt_rand(), 'u2' => mt_rand(), 'u3' => mt_rand()]);
        $this->assertTrue($id3 === 3);
        $elem = current($table->get($id1));
        $this->assertTrue($elem['u1'] === 'foo');
    }

    /**
     * @test
     */
    public function test_case_storage_filter()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text1' => [
                    ],
                    'text2' => [
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        foreach (range(1, 1000) as $_) {
            $table->insert($_ = ['text1' => mt_rand() % 5, 'text2' => mt_rand() % 5]);
        }
        $elems = $table->filter(['text1' => 1]);
        $this->assertTrue(100 < count($elems) && count($elems) < 300);
        $this->assertTrue(
            count($table->filter(['text1' => 0])) +
            count($table->filter(['text1' => 1])) +
            count($table->filter(['text1' => 2])) +
            count($table->filter(['text1' => 3])) +
            count($table->filter(['text1' => 4])) ===
            1000
        );
        [$id, $row] = [key($elems), current($elems)];
        $this->assertTrue($id > 0);
        $this->assertTrue(isset($row['text1']));
        $this->assertTrue(isset($row['text2']));
        $this->assertTrue(!isset($row['table_id']));
        $elem = current($table->get($id, 'text2'));
        $this->assertTrue(!isset($elem['text1']));
        $this->assertTrue(isset($elem['text2']));
        $elem = current($table->get($id));
        $this->assertTrue(isset($elem['text1']));
        $this->assertTrue(isset($elem['text2']));
        $elem = current($table->get($id, []));
        $this->assertTrue(!isset($elem['text1']));
        $this->assertTrue(!isset($elem['text2']));
    }

    /**
     * @test
     */
    public function test_case_storage_iterate_1()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text1' => [
                    ],
                    'text2' => [
                    ],
                ],
                'modifiers' => [
                    '~^text3$~i' => function ($match) {
                        return [
                            'text2',
                            ['get_pattern' => 'CONCAT(%s, \'hello\')'],
                        ];
                    },
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        foreach (range(1, 2E3) as $_) {
            $table->insert($_ = ['text1' => mt_rand() % 5, 'text2' => mt_rand() % 5]);
        }
        $ids = [];
        foreach ($table->iterateAsGenerator(['fields' => 'text3']) as $id => $row) {
            $ids[] = $id;
        }
        $this->assertTrue(isset($row['text3']));
        $this->assertTrue(!!preg_match('~^\dhello$~', $row['text3']));
        $this->assertTrue(min($ids) === 1);
        $this->assertTrue(max($ids) === 2000);
        $this->assertTrue(count($ids) === 2000);
        $all = array_fill_keys($ids, true);
        $ids = [];
        foreach ($table->iterateAsGenerator(['rand' => true]) as $id => $row) {
            $ids[] = $id;
            unset($all[$id]);
        }
        $this->assertTrue($ids[0] !== 1 && $ids[1999] !== 2000);
        $this->assertTrue(!$all);
        $this->assertTrue(min($ids) === 1);
        $this->assertTrue(max($ids) === 2000);
        $this->assertTrue(count($ids) === 2000);
        $this->assertTrue(count(array_unique($ids)) === 2000);
    }

    /**
     * @test
     */
    public function test_case_storage_iterate_2()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text1' => [],
                ],
                'get_shards_by_id' => function ($id, $shards) {
                    $c = count($shards);
                    $keys = array_keys($shards);
                    $id %= $c;
                    $id -= 1;
                    $id = $id < 0 ? $id + $c : $id;
                    $key = $keys[$id];
                    return [$shards[$key]];
                },
                'get_write_shards_by_values' => function ($values, $shards) {
                    return [$shards[array_rand($shards)]];
                },
                'primary_key_start_with' => function ($shard, $shards) {
                    $index = (int) array_search($shard, $shards);
                    return 1 + $index;
                },
                'primary_key_increment_by' => function ($shard, $shards) {
                    return count($shards);
                },
            ],
        ]);
        $table = $storage->table();
        $table->create();
        foreach (range(1, 2E3) as $_) {
            $table->insert($_ = ['text1' => mt_rand() % 5]);
        }
        $ids = [];
        foreach ($table->iterateAsGenerator() as $id => $row) {
            $ids[] = $id;
        }
        $this->assertTrue(min($ids) === 1);
        $this->assertTrue(100 * abs(max($ids) - 2E3) / 2E3 < 10);
        $this->assertTrue(count($ids) === 2000);
        $this->assertTrue(count(array_unique($ids)) === 2000);
    }

    /**
     * @test
     */
    public function test_case_storage_update()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text1' => [],
                ],
                'get_shards_by_id' => function ($id, $shards) {
                    $c = count($shards);
                    $keys = array_keys($shards);
                    $id %= $c;
                    $id -= 1;
                    $id = $id < 0 ? $id + $c : $id;
                    $key = $keys[$id];
                    return [$shards[$key]];
                },
                'get_write_shards_by_values' => function ($values, $shards) {
                    return [$shards[array_rand($shards)]];
                },
                'primary_key_start_with' => function ($shard, $shards) {
                    $index = (int) array_search($shard, $shards);
                    return 1 + $index;
                },
                'primary_key_increment_by' => function ($shard, $shards) {
                    return count($shards);
                },
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id = $table->insert(['text1' => 'foo']);
        $ok = $table->update($id, ['text1' => 'bar']);
        $this->assertTrue($ok);
        $this->assertTrue(current($table->get($id))['text1'] === 'bar');
    }

    /**
     * @test
     */
    public function test_case_storage_modifiers_1()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text1' => [],
                    'json1' => [
                        'type' => TableDefinition::TYPE_JSON,
                    ],
                ],
                'modifiers' => [
                    '~^(.*)\|trim$~i' => function ($match) {
                        $append = ['set' => function ($v) { return trim($v); }];
                        return [$match[1], $append];
                    },
                    '~^(.*)\{\}$~i' => function ($match) {
                        $append = [
                            'set_pattern' => '%s || ?',
                        ];
                        return [$match[1], $append];
                    },
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id = $table->insert(['text1|Trim' => ' foo ']);
        $this->assertTrue(current($table->get($id))['text1'] === 'foo');
        $id = $table->insert(['json1' => ['a' => 1, 'b' => 2]]);
        $this->assertTrue(current($table->get($id))['json1']['a'] === 1);
        $table->update($id, ['json1{}' => ['c' => 3]]);
        $this->assertTrue(current($table->get($id))['json1']['a'] === 1);
        $this->assertTrue(current($table->get($id))['json1']['c'] === 3);
    }

    /**
     * @test
     */
    public function test_case_storage_modifiers_2()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'json1' => [
                        'type' => TableDefinition::TYPE_JSON,
                        'get' => function ($v) {
                            return ['x' => 1] + $v;
                        },
                        'set' => function ($v) {
                            return ['y' => 1] + $v;
                        },
                    ],
                ],
                'modifiers' => [
                    '~^(.*)\|ab$~i' => function ($match) {
                        $append = [
                            'get' => function ($v) {
                                return ['a' => 1] + $v;
                            },
                            'set' => function ($v) {
                                return ['b' => 1] + $v;
                            },
                        ];
                        $append['alias'] = 'rnd';
                        return [$match[1], $append];
                    },
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id = $table->insert(['json1' => ['m' => 1]]);
        $json1 = current($table->get($id))['json1'];
        $this->assertTrue(isset($json1['x'], $json1['y'], $json1['m']));
        $id = $table->insert(['json1|ab' => ['m' => 2]]);
        $json1 = current($table->get($id))['json1'];
        $this->assertTrue(isset($json1['x'], $json1['b'], $json1['m']));
        $this->assertTrue(!isset($json1['a']));
        $elem = current($table->get($id, 'json1|ab'));
        $this->assertTrue(isset($elem['rnd']['a']));
    }

    /**
     * @test
     */
    public function test_case_storage_modifiers_3()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'int' => [
                        'type' => TableDefinition::TYPE_INT,
                        'set_pattern' => '? * 2',
                        'get_pattern' => '%s / 2',
                    ],
                ],
                'modifiers' => [
                    '~^(.*)\|m$~i' => function ($match) {
                        $append = [
                            'set_pattern' => '? ^ 5',
                            'get_pattern' => '%s ^ 0.2',
                        ];
                        $append['alias'] = 'rnd';
                        return [$match[1], $append];
                    },
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id = $table->insert(['int' => 7]);
        $int = current($table->get($id))['int'];
        $this->assertTrue($int === 7);
        $id = $table->insert(['int|m' => 8]);
        $rnd = current($table->get($id, 'int|m'))['rnd'];
        $this->assertTrue($rnd == 8);
    }

    /**
     * @test
     */
    public function test_case_storage_foreign_key()
    {
        $storage = $this->getStorage([
            'parent' => [
                'fields' => [
                    'int' => [
                        'type' => TableDefinition::TYPE_INT,
                    ],
                ],
            ],
            'child' => [
                'fields' => [
                    'int' => [
                        'type' => TableDefinition::TYPE_INT,
                    ],
                    'parent_id' => [
                        'type' => TableDefinition::TYPE_FOREIGN_KEY,
                        'references' => '"parent" ("parent_id")',
                    ],
                ],
            ],
        ]);
        $parent = $storage->parent();
        $parent->create();
        $pid = $parent->insert();
        $this->assertTrue($pid > 0);
        $child = $storage->child();
        $child->create();
        $cid = $child->insert(['parent_id' => $pid]);
        $this->assertTrue($cid > 0);
        $elem = $child->get($cid);
        $this->assertTrue(key($elem) === $cid);
        $this->assertTrue(current($elem)['parent_id'] === $pid);
        $this->assertTrue($parent->reid($pid, 70));
        $elem = $child->get($cid);
        $this->assertTrue(current($elem)['parent_id'] === 70);
    }

    /**
     * @test
     */
    public function test_case_storage_delete()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text' => [
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $tid = $table->insert();
        $this->assertTrue($tid > 0);
        $this->assertTrue(!empty($table->get($tid)));
        $table->delete($tid);
        $this->assertTrue(empty($table->get($tid)));
    }

    /**
     * @test
     */
    public function test_case_storage_multiple_ids()
    {
        $storage = $this->getStorage([
            'table' => [
                'fields' => [
                    'text' => [
                    ],
                ],
                'is_cacheable' => mt_rand() % 2,
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $tid1 = $table->insert();
        $tid2 = $table->insert();
        $this->assertTrue($tid1 > 0 && $tid2 > 0);
        $elems = $table->get($tid1, $tid2);
        $this->assertTrue(isset($elems[$tid1], $elems[$tid2]));
    }
}
