<?php

namespace Tests;

use Ejz\Storage;
use Ejz\Repository;

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
}
