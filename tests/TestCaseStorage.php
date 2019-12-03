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
        // $id2 = $table->insert(['blob' => str_repeat(chr(1), 1E7)]);
        // $elem = $table->get($id2);
        // $this->assertTrue(strlen($elem['blob']) == 1E7);
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
        $id1 = $table->insert(['u1' => 'foo']);
        try {
            $id2 = $table->insert(['u1' => 'foo']);
        } catch (\Amp\Postgres\QueryExecutionError $e) {
            $id2 = 0;
        }
        // $this->assertTrue($id2 === 0);
        // $elem = $table->get($id1);
        // $this->assertTrue($elem['text'] === 'hello');
        // $this->assertTrue($elem['json'] === ['hi']);
    }

// var_dump($elem);
        // $this->assertTrue($elem['blob'] === chr(0));
        // // $id2 = $table->insert(['blob' => str_repeat(chr(1), 1E7)]);
        // // $elem = $table->get($id2);
        // // $this->assertTrue(strlen($elem['blob']) == 1E7);
        // $id3 = $table->insert(['blob' => '']);
        // $elem = $table->get($id3);
        // $this->assertTrue($elem['blob'] === '');
        // $id4 = $table->insert();
        // $elem = $table->get($id4);
        // $this->assertTrue($elem['blob'] === '');
    // $table = [
    //         'fields' => [
    //             'table_id' => [
    //                 'type' => 'primary',
    //             ],
    //             'email' => [
    //                 'type' => 'text',
    //                 'unique' => 'email',
    //             ],
    //         ],
    //     ];
    //     $storage = $this->getStorage(compact('table'));
    //     $table = $storage->table();
    //     $table->create();
    //     $id1 = $table->insert(['email' => 1]);
    //     $this->assertTrue($id1 === 1);
    //     $id2 = $table->insert(['email' => 2]);
    //     $this->assertTrue($id2 === 2);
    //     $id3 = $table->insert(['email' => 3]);
    //     $this->assertTrue($id3 === 3);
    //     $t1 = $table->get(1);
    //     $t2 = $table->get(2);
    //     $t3 = $table->get(3);
        
        // $d = $this->database;
        // $fields = [
        //     'tt' => [
        //         'type' => 'integer',
        //         'default' => 0,
        //     ],
        // ];
        // $table = [$fields, []];
        // $storage = $this->getStorage(compact('table'));
        // $table = $storage->table();
        // $table->create();
        // $fields = $storage->getPool()->db(0)->fields('table');
        
        // $fields = $storage->getPool()->fields('table')[0];
        // $this->assertTrue(isset($fields['tt']));
        // $this->assertTrue($fields['tt']['type'] === 'integer');
        
        // $id1 = $table->insert([]);
        // $this->assertTrue($id1 === 1);
        // $id2 = $table->insert([]);
        // $this->assertTrue($id2 === 2);
        // $id3 = $table->insert([]);
        // $this->assertTrue($id3 === 3);
}
