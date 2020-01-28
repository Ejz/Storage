<?php

namespace Tests;

use Ejz\Type;
use Ejz\Bean;
use Ejz\Storage;
use Ejz\Index;
use RuntimeException;

use function Amp\Promise\wait;
use function Amp\Promise\all;
use function Container\getStorage;
use function Container\getBitmap;

class TestCaseStorageBugs extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_storage_bugs_primary_cluster()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ] + Storage::getPrimarySecondaryClusterConfig(0),
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync(['field1' => 'foo']);
        foreach (range(1, 100) as $_) {
            $field1 = $table->get([$id])->generator()->current()->field1;
            $this->assertTrue($field1 === 'foo');
        }
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_changed_1()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync(['field1' => 'foo']);
        $this->assertTrue($table->get([$id])->generator()->current()->field1 === 'foo');
        $bean = $table->get([$id])->generator()->current();
        $bean->field1 = 'bar1';
        $this->assertTrue($bean->updateSync());
        $this->assertFalse($bean->updateSync());
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_changed_2()
    {
        $storage = getStorage([
            'table' => [
                'table' => 'table' . mt_rand(),
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $id = $table->insertSync(['field1' => 'foo']);
        $this->assertTrue($table->get([$id])->generator()->current()->field1 === 'foo');
        $bean = $table->get([$id])->generator()->current();
        $bean->field1 = 'bar';
        $bean->updateSync();
        $this->assertTrue($table->get([$id])->generator()->current()->field1 === 'bar');
        [$tbl, $pk] = [$table->getTable(), $table->getPk()];
        $table->getPool()->execSync("UPDATE {$tbl} SET field1 = ?", 'bar1');
        $this->assertTrue($table->get([$id])->generator()->current()->field1 === 'bar1');
        $bean->updateSync();
        $this->assertTrue($table->get([$id])->generator()->current()->field1 === 'bar1');
        $bean->updateSync(true);
        $this->assertTrue($table->get([$id])->generator()->current()->field1 === 'bar');
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_bean_insert()
    {
        $storage = getStorage([
            'table' => [
                'table' => 'table' . mt_rand(),
                'fields' => [
                    'field1' => Type::string(),
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $bean = $table->getBean();
        $id = $bean->insertSync();
        $this->assertTrue($id > 0);
        $this->assertTrue($bean->getId() === $id);
        $this->assertTrue($bean->id === $id);
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_bitmap()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::bool(),
                ],
                'bitmap' => [
                    'fields' => [
                        'field1' => Type::bitmapBool(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $table->bitmapCreate();
        $ids = [];
        foreach (range(1, 1000) as $_) {
            $ids[] = $table->insertSync(['field1' => mt_rand(0, 1)]);
        }
        $table->bitmapPopulate();
        if (mt_rand(0, 1)) {
            foreach ($table->search('*')->generator() as $id => $bean) {
                $_ = array_search($id, $ids);
                unset($ids[$_]);
            }
        } else {
            foreach ($table->search('@field1:1')->generator() as $id => $bean) {
                $_ = array_search($id, $ids);
                unset($ids[$_]);
            }
            foreach ($table->search('@field1:0')->generator() as $id => $bean) {
                $_ = array_search($id, $ids);
                unset($ids[$_]);
            }
        }
        $this->assertTrue($ids === []);
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_cursor()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::bool(),
                ],
                'bitmap' => [
                    'fields' => [
                        'field1' => Type::bitmapBool(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $table->bitmapCreate();
        $ids = [];
        foreach (range(1, 1000) as $_) {
            $ids[] = $table->insertSync(['field1' => mt_rand(0, 1)]);
        }
        $table->bitmapPopulate();
        [$size, $cursor] = $table->bitmapSearch('*');
        while ($ids) {
            $c1 = count($ids);
            $chuck = $table->bitmapIterator($cursor, mt_rand(1, 4))->generator();
            foreach ($chuck as $id => $bean) {
                $_ = array_search($id, $ids);
                $this->assertTrue($_ !== false);
                unset($ids[$_]);
            }
            $c2 = count($ids);
            $this->assertTrue($c2 < $c1);
        }
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_handle_values()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'field1' => Type::bool(),
                    'field2' => Type::bool(),
                ],
                'bitmap' => [
                    'fields' => [
                        'field1' => Type::bitmapBool(),
                    ],
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $table->bitmapCreate();
        $ids = [];
        foreach (range(1, 100) as $_) {
            $ids[] = $table->insertSync(['field1' => mt_rand(0, 1)]);
        }
        $table->bitmapPopulate();
        $this->assertTrue(true);
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_new_type_compressed_binary()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'compressed' => Type::compressedBinary(),
                ],
            ],
            'tablenull' => [
                'fields' => [
                    'compressed' => Type::compressedBinary(true),
                ],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $values = [
            '',
            'string',
            '"!"\'',
            str_repeat(md5(mt_rand()), 1E4),
        ];
        foreach ($values as $value) {
            $id = $table->insertSync(['compressed' => $value]);
            $bean = $table->get([$id])->generator()->current();
            $this->assertTrue($bean->compressed === $value);
        }
        /* @TODO
        $tablenull = $storage->tablenull();
        $tablenull->createSync();
        $id = $tablenull->insertSync(['compressed' => null]);
        $bean = $tablenull->get([$id])->generator()->current();
        $this->assertTrue($bean->compressed === null);
        */
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_bitmap_search()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'bool' => Type::bool(),
                    'score' => Type::int(),
                ],
                'bitmap' => [
                    'fields' => [
                        'bool' => Type::bitmapBool(),
                    ],
                ],
                'getSortScore' => function ($bean) {
                    return $bean->score;
                },
            ] + Storage::getShardsClusterConfig(),
        ]);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        foreach (range(1, mt_rand(300, 1300)) as $_) {
            $ids[] = $table->insertSync(['bool' => mt_rand(0, 1), 'score' => mt_rand(1, 100)]);
        }
        $table->sort();
        $table->bitmapCreate();
        $table->bitmapPopulate();
        $ids = array_flip($ids);
        foreach ($table->search('*')->generator() as $id => $bean) {
            var_dump($bean->score);
            continue;
            $score = $score ?? $bean->score;
            $this->assertTrue($score <= $bean->score, "{$score} <= {$bean->score}");
            $score = $bean->score;
        }
        return;
        // $iterator = ;
        $values = iterator_to_array($iterator);
        $count = 0;
        foreach ($values as $id => $bean) {
            $this->assertTrue(isset($ids[$id]));
            unset($ids[$id]);
            $count++;
            $this->assertTrue($count === $id);
            if ($count === 450) {
                break;
            }
        }
        $this->assertTrue($ids === []);
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_get_order_with_cache()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'int' => Type::int(),
                ],
                'cache' => [],
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        foreach (range(1, 1000) as $_) {
            $ids[] = $table->insertSync(['int' => mt_rand(1, 1000)]);
        }
        $allids = $ids;
        $table->sort();
        $table->bitmapCreate();
        $table->bitmapPopulate();
        $ids = [];
        $min = null;
        $max = null;
        foreach ($table->search('*')->generator() as $id => $bean) {
            $min = $min ?? $bean->int;
            $max = $max ?? $bean->int;
            $this->assertTrue(
                $min <= $bean->int && $bean->int <= $max,
                "{$min} <= {$bean->int} <= $max"
            );
            $min = min($min, $bean->int);
            $max = max($max, $bean->int);
            $ids[] = $id;
            if (count($ids) === 500) {
                break;
            }
        }
        // $this->assertEquals($ids, array_map('intval', range(1, 500)));
        // foreach ($table->search('*')->generator() as $id => $_) {
        //     unset($allids[array_search($id, $allids)]);
        // }
        // $this->assertTrue($allids === []);
    }

    /**
     * @test
     */
    public function test_case_storage_bugs_order_after_sort()
    {
        return;
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'int' => Type::int(),
                ],
                'bitmap' => [
                    'fields' => [],
                ],
                'getSortScore' => function ($values) {
                    return $values->getValues()['int'];
                },
            ] + Storage::getShardsClusterConfig(),
        ]);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        foreach (range(1, 1000) as $_) {
            $ids[] = $table->insertSync(['int' => mt_rand(1, 1000)]);
        }
        $allids = $ids;
        $table->sort();
        $table->bitmapCreate();
        $table->bitmapPopulate();
        $ids = [];
        $min = null;
        $max = null;
        foreach ($table->search('*')->generator() as $id => $bean) {
            $min = $min ?? $bean->int;
            $max = $max ?? $bean->int;
            $this->assertTrue(
                $min <= $bean->int && $bean->int <= $max,
                "{$min} <= {$bean->int} <= $max"
            );
            $min = min($min, $bean->int);
            $max = max($max, $bean->int);
            $ids[] = $id;
            if (count($ids) === 500) {
                break;
            }
        }
        $this->assertEquals($ids, array_map('intval', range(1, 500)));
        foreach ($table->search('*')->generator() as $id => $_) {
            unset($allids[array_search($id, $allids)]);
        }
        $this->assertTrue($allids === []);
    }
}
