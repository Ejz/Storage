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
            $chuck = $table->bitmapCursor($cursor, mt_rand(1, 4));
            foreach ($chuck as $id) {
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
    public function test_case_storage_bugs_bitmap_iterate_order()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'string' => Type::bool(),
                ],
                'bitmap' => [
                    'fields' => [
                        'string' => Type::bitmapBool(),
                    ],
                ],
            ] + Storage::getShardsClusterConfig(),
        ]);
        $table = $storage->table();
        $table->createSync();
        foreach (range(1, 1000) as $_) {
            $table->insertSync(['string' => mt_rand(0, 1)]);
        }
        $table->bitmapCreate();
        $table->bitmapPopulate();
        $iterator = $table->search('*')->generator();
        $values = iterator_to_array($iterator);
        $count = 0;
        $ids = array_flip(range(1, 450));
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
}
