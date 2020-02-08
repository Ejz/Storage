<?php

namespace Tests;

use Ejz\Storage;
use Ejz\Repository;
use Ejz\Pool;
use Ejz\Type;

class TestCaseStorageRefactor extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_storage_refactor_common_1()
    {
        $storage = \Container\getStorage([
            'table' => [],
        ]);
        $this->assertTrue($storage instanceof Storage);
        $table = $storage->table();
        $this->assertTrue($table instanceof Repository);
        $this->assertFalse($table->hasBitmap());
        $this->assertFalse($table->hasDatabase());
    }

    /**
     * @test
     */
    public function test_case_storage_refactor_common_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [],
            ],
        ]);
        $table = $storage->table();
        $this->assertTrue($table->hasDatabase());
        $this->assertFalse($table->hasBitmap());
        $this->assertTrue($table->getBitmapPool()->names() === []);
        $table->createSync();
        $db = $table->getDatabasePool()->random();
        $tables = $db->tablesSync();
        $this->assertTrue(in_array($table, $tables));
    }

    /**
     * @test
     *
     * @param string   $cluster
     * @param callable $checker
     *
     * @dataProvider provider_test_case_storage_refactor_cluster_1
     */
    public function test_case_storage_refactor_cluster_1(string $cluster, callable $checker)
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'cluster' => $cluster,
                ],
            ],
        ]);
        $storage->createSync();
        $names = $storage->getDatabasePool()->names();
        $mod = function ($pool) use ($names) {
            $collect = [];
            foreach ($pool->names() as $name) {
                $collect[] = array_search($name, $names);
            }
            return $collect;
        };
        $checker(
            $mod($storage->table()->getDatabasePool(Pool::POOL_WRITABLE)),
            $mod($storage->table()->getDatabasePool(Pool::POOL_PRIMARY)),
            $mod($storage->table()->getDatabasePool(Pool::POOL_SECONDARY))
        );
    }

    /**
     * @return array
     */
    public function provider_test_case_storage_refactor_cluster_1(): array
    {
        $cases = [];
        $cases[0] = [
            'w:*',
            function ($w, $p, $s) {
                $this->assertTrue($w === [0, 1, 2]);
                $this->assertTrue($p === [0, 1, 2]);
                $this->assertTrue($s === []);
            },
        ];
        $cases[1] = [
            'w:0,2',
            function ($w, $p, $s) {
                $this->assertTrue($w === [0, 2]);
                $this->assertTrue($p === [0, 2]);
                $this->assertTrue($s === []);
            },
        ];
        $cases[2] = [
            'w:!0',
            function ($w, $p, $s) {
                $this->assertTrue($w === [1, 2]);
                $this->assertTrue($p === [1, 2]);
                $this->assertTrue($s === []);
            },
        ];
        $cases[3] = [
            'w:!0;p:!0;',
            function ($w, $p, $s) {
                $this->assertTrue($w === [1, 2]);
                $this->assertTrue($p === [2]);
                $this->assertTrue($s === [1]);
            },
        ];
        return $cases;
    }

    /**
     * @test
     *
     * @param string   $cluster
     * @param callable $checker
     *
     * @dataProvider provider_test_case_storage_refactor_cluster_2
     */
    public function test_case_storage_refactor_cluster_2(string $cluster, callable $checker)
    {
        $table = 'table' . mt_rand();
        $pk = 'pk' . mt_rand();
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'table' => $table,
                    'pk' => $pk,
                    'cluster' => $cluster,
                    'fields' => [
                        'num' => Type::int(),
                    ],
                ],
            ],
        ]);
        $storage->createSync();
        foreach (range(1, 10) as $_) {
            $storage->table()->insertSync(['num' => $_]);
        }
        $names = $storage->getDatabasePool()->names();
        $collect = [];
        $idx = 0;
        foreach ($names as $name) {
            $db = $storage->getDatabasePool()->instance($name);
            $collect[$idx] = $db->colSync("SELECT CONCAT({$pk}, '-', num) FROM {$table}");
            $collect[$idx] = implode(',', $collect[$idx]);
            $idx++;
        }
        $checker($collect);
    }

    /**
     * @return array
     */
    public function provider_test_case_storage_refactor_cluster_2(): array
    {
        $cases = [];
        $cases[0] = [
            '',
            function (array $ids) {
                print_r($ids);
                $this->assertTrue(true);
            },
        ];
        return $cases;
    }
}
