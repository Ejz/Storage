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

class TestCaseCluster extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_storage_cluster_1()
    {
        $storage = getStorage([
            'table' => [
                'database' => [
                    'cluster' => 'w:0;',
                    'table' => 'table',
                ],
            ],
        ]);
        $pool = $storage->getDatabasePool();
        $names = $pool->names();
        $table = $storage->table();
        $table->createSync();
        $this->assertTrue($pool->instance($names[0])->tableExistsSync('table'));
        $this->assertFalse($pool->instance($names[1])->tableExistsSync('table'));
    }
}
