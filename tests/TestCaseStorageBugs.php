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
    public function test_case_storage_primary_cluster_bug()
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
    public function test_case_storage_changed_bug_1()
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
    public function test_case_storage_changed_bug_2()
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
    public function test_case_storage_bean_insert_bug()
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
    public function test_case_storage_bitmap_bug()
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
}
