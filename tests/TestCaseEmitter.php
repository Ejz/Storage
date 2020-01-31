<?php

namespace Tests;

use Amp\Iterator;
use Ejz\Emitter;
use Ejz\Storage;
use Ejz\Type;
use Generator;

class TestCaseEmitter extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_emitter_from_iterable()
    {
        $values = [[1, 2], [3, 4], [5, 6]];
        $emitter = Emitter::fromIterable($values);
        $this->assertTrue($emitter instanceof Emitter);
        $generator = $emitter->generator();
        $this->assertTrue($generator instanceof Generator);
        $values = iterator_to_array($generator);
        $this->assertEquals([1 => 2, 3 => 4, 5 => 6], $values);
    }

    /**
     * @test
     */
    public function test_case_emitter_get_iterator_with_ids_order_1()
    {
        $values = [[1, ['a']], [2, ['b']], [3, ['c']]];
        $i = Emitter::getIteratorWithIdsOrder(Emitter::fromIterable($values), [1, 3, 2]);
        $values = iterator_to_array($i->generator());
        $this->assertEquals([1 => ['a'], 2 => ['b'], 3 => ['c']], $values);
    }

    /**
     * @test
     */
    public function test_case_emitter_get_iterator_with_ids_order_2()
    {
        $storage = \Container\getStorage([
            'table' => [
                'database' => [
                    'fields' => [
                        'score' => Type::int(),
                    ],
                    'getSortScore' => function ($bean) {
                        return $bean->score;
                    },
                ] + Storage::getShardsClusterConfig(),
            ],
        ]);
        $table = $storage->table();
        $table->createSync();
        $ids = [];
        foreach (range(1, mt_rand(300, 1300)) as $_) {
            $ids[] = $table->insertSync(['score' => mt_rand(1, 100)]);
        }
        $table->sort();
        $iterator = $table->get(range(mt_rand(1, 100), mt_rand(300, 1000)));
        sort($ids);
        $iterator = Emitter::getIteratorWithIdsOrder($iterator, $ids);
        foreach ($iterator->generator() as $id => $bean) {
            $ex = $ex ?? $id;
            $this->assertTrue($id >= $ex, "{$id} >= {$ex}");
            $ex = $id;
        }
    }
}
