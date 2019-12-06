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
                    ],
                ],
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return $shards;
                },
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
        $id3 = $table->insert();
        $this->assertEquals(3, $id3);
        $elem = $table->get($id2);
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
                        'get_pattern' => 'encode(%s, \'base64\')',
                        'get' => function ($val) {
                            return base64_decode($val);
                        },
                        'set_pattern' => 'decode(?, \'base64\')::BYTEA',
                        'set' => function ($val) {
                            return base64_encode($val);
                        },
                    ],
                ],
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return $shards;
                },
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id1 = $table->insert(['blob' => chr(0)]);
        $elem = $table->get($id1);
        $this->assertTrue($elem['blob'] === chr(0));
        $id2 = $table->insert(['blob' => str_repeat(chr(1), 1E7)]);
        $elem = $table->get($id2);
        $this->assertTrue(strlen($elem['blob']) == 1E7);
        $id3 = $table->insert(['blob' => '']);
        $elem = $table->get($id3);
        $this->assertTrue($elem['blob'] === '');
        $id4 = $table->insert();
        $elem = $table->get($id4);
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
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return $shards;
                },
            ],
        ]);
        $table = $storage->table();
        $table->create();
        $id1 = $table->insert(['text' => 'hello', 'json' => ['hi']]);
        $elem = $table->get($id1);
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
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return $shards;
                },
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
        $elem = $table->get($id1);
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
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return $shards;
                },
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
        $elem = $table->get($id, 'text2');
        $this->assertTrue(!isset($elem['text1']));
        $this->assertTrue(isset($elem['text2']));
        $elem = $table->get($id);
        $this->assertTrue(isset($elem['text1']));
        $this->assertTrue(isset($elem['text2']));
        $elem = $table->get($id, []);
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
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return $shards;
                },
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
                'get_shards' => function (?int $id, array $values, array $shards) {
                    return [$shards[array_rand($shards)]];
                },
                'primary_key_start_with' => function ($shard, $shards) {
                    return 1 + ((int) $shard);
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
                'get_shards' => function (?int $id, array $values, array $shards) {
                    $c = count($shards);
                    $keys = array_keys($shards);
                    if (!isset($id)) {
                        $key = $keys[mt_rand(0, $c - 1)];
                    } else {
                        $id %= $c;
                        $id -= 1;
                        $id = $id < 0 ? $id + $c : $id;
                        $key = $keys[$id];
                    }
                    return [$shards[$key]];
                },
                'primary_key_start_with' => function ($shard, $shards) {
                    return 1 + ((int) array_search($shard, $shards));
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
        $this->assertTrue($table->get($id)['text1'] === 'bar');
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
        $this->assertTrue($table->get($id)['text1'] === 'foo');
        $id = $table->insert(['json1' => ['a' => 1, 'b' => 2]]);
        $this->assertTrue($table->get($id)['json1']['a'] === 1);
        $table->update($id, ['json1{}' => ['c' => 3]]);
        $this->assertTrue($table->get($id)['json1']['a'] === 1);
        $this->assertTrue($table->get($id)['json1']['c'] === 3);
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
        $json1 = $table->get($id)['json1'];
        $this->assertTrue(isset($json1['x'], $json1['y'], $json1['m']));
        $id = $table->insert(['json1|ab' => ['m' => 2]]);
        $json1 = $table->get($id)['json1'];
        $this->assertTrue(isset($json1['x'], $json1['b'], $json1['m']));
        $this->assertTrue(!isset($json1['a']));
        $elem = $table->get($id, 'json1|ab');
        $this->assertTrue(isset($elem['rnd']['a']));
    }
}
