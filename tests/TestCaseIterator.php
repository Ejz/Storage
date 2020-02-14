<?php

namespace Tests;

use Ejz\Iterator;

class TestCaseIterator extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_iterator_0()
    {
        $source1 = [1, 2, 3];
        $source2 = function ($e) {
            yield \Amp\delay(100);
            yield $e(1);
            yield \Amp\delay(100);
            yield $e(2);
            yield \Amp\delay(100);
            yield $e(3);
        };
        $iterator = new Iterator(mt_rand(0, 1) ? $source1 : $source2);
        $cmp = [];
        while ($iterator->advanceSync()) {
            $cmp[] = $iterator->getCurrent();
        }
        $this->assertEquals([1, 2, 3], $cmp);
    }

    /**
     * @test
     */
    public function test_case_iterator_1()
    {
        $source1 = [[1, 'v1'], [2, 'v2'], [3, 'v3']];
        $source2 = function ($e) {
            yield \Amp\delay(100);
            yield $e([1, 'v1']);
            yield \Amp\delay(100);
            yield $e([2, 'v2']);
            yield \Amp\delay(100);
            yield $e([3, 'v3']);
        };
        $iterator = new Iterator(mt_rand(0, 1) ? $source1 : $source2);
        $cmp = [];
        foreach ($iterator as $key => $value) {
            $cmp[$key] = $value;
        }
        $this->assertEquals([1 => 'v1', 2 => 'v2', 3 => 'v3'], $cmp);
    }

    /**
     * @test
     */
    public function test_case_iterator_2()
    {
        $iterator = new Iterator([]);
        $this->assertTrue(!$iterator->advanceSync());
        $iterator = new Iterator([]);
        $this->assertTrue($iterator->current() === null);
    }

    /**
     * @test
     */
    public function test_case_iterator_merge()
    {
        $iterator1 = [[1, 'v1'], [3, 'v3'], [6, 'v6']];
        $iterator2 = [[2, 'v2']];
        $iterator3 = [[100, 'v100']];
        $iterator = Iterator::merge([
            new Iterator($iterator1),
            new Iterator($iterator2),
            new Iterator($iterator3),
        ], function ($a, $b) {
            return $a[0] - $b[0];
        });
        $values = iterator_to_array($iterator);
        $this->assertTrue(array_keys($values) === [1, 2, 3, 6, 100]);
    }
}
// /**
//  * @test
//  */
// public function test_case_emitter_from_iterable()
// {
//     $values = [[1, 2], [3, 4], [5, 6]];
//     $emitter = Emitter::fromIterable($values);
//     $this->assertTrue($emitter instanceof Emitter);
//     $generator = $emitter->generator();
//     $this->assertTrue($generator instanceof Generator);
//     $values = iterator_to_array($generator);
//     $this->assertEquals([1 => 2, 3 => 4, 5 => 6], $values);
// }

// /**
//  * @test
//  */
// public function test_case_emitter_get_iterator_with_ids_order_1()
// {
//     $values = [[1, ['a']], [2, ['b']], [3, ['c']]];
//     $i = Emitter::getIteratorWithIdsOrder(Emitter::fromIterable($values), [1, 3, 2]);
//     $values = iterator_to_array($i->generator());
//     $this->assertEquals([1 => ['a'], 2 => ['b'], 3 => ['c']], $values);
// }

// /**
//  * @test
//  */
// public function test_case_emitter_get_iterator_with_ids_order_2()
// {
//     $storage = \Container\getStorage([
//         'table' => [
//             'database' => [
//                 'fields' => [
//                     'score' => Type::int(),
//                 ],
//                 'getSortScore' => function ($bean) {
//                     return $bean->score;
//                 },
//             ] + Storage::getShardsClusterConfig(),
//         ],
//     ]);
//     $table = $storage->table();
//     $table->createSync();
//     $ids = [];
//     foreach (range(1, mt_rand(300, 1300)) as $_) {
//         $ids[] = $table->insertSync(['score' => mt_rand(1, 100)]);
//     }
//     $table->sort();
//     $iterator = $table->get(range(mt_rand(1, 100), mt_rand(300, 1000)));
//     sort($ids);
//     $iterator = Emitter::getIteratorWithIdsOrder($iterator, $ids);
//     foreach ($iterator->generator() as $id => $bean) {
//         $ex = $ex ?? $id;
//         $this->assertTrue($id >= $ex, "{$id} >= {$ex}");
//         $ex = $id;
//     }
// }