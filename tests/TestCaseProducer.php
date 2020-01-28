<?php

namespace Tests;

use Amp\Iterator;
use Ejz\Producer;
use Ejz\Storage;
use Ejz\Type;

use function Container\getStorage;

class TestCaseProducer extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_producer_is_cacheable()
    {
        $cache = \Container\getCache();
        $values = [1, 2.1, '3', null, true, false, [], ['foo'], ['foo' => 'bar'], new \stdClass()];
        $iterator = Iterator\fromIterable($values, 50);
        \Amp\Promise\wait($iterator->advance());
        $cur = $iterator->getCurrent();
        $this->assertTrue($cur === 1);
        $cache->set('_', $iterator);
        $iterator = $cache->get('_');
        var_dump($iterator);
        \Amp\Promise\wait($iterator->advance());
        $cur = $iterator->getCurrent();
        $this->assertTrue($cur === 2.1);
        return;
        // $i = Producer::getIteratorWithIdsOrder(, [1, 3, 2]);
        // $values = iterator_to_array($i->generator());
        // $this->assertEquals([1 => ['a'], 2 => ['b'], 3 => ['c']], $values);
    }

    /**
     * @test
     */
    public function test_case_producer_get_iterator_with_ids_order_1()
    {
        $values = [[1, ['a']], [2, ['b']], [3, ['c']]];
        $i = Producer::getIteratorWithIdsOrder(Iterator\fromIterable($values, 50), [1, 3, 2]);
        $values = iterator_to_array($i->generator());
        $this->assertEquals([1 => ['a'], 2 => ['b'], 3 => ['c']], $values);
    }

    /**
     * @test
     */
    public function test_case_producer_get_iterator_with_ids_order_2()
    {
        $storage = getStorage([
            'table' => [
                'fields' => [
                    'score' => Type::int(),
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
            $ids[] = $table->insertSync(['score' => mt_rand(1, 100)]);
        }
        $table->sort();
        $iterator = $table->get(range(mt_rand(1, 100), mt_rand(300, 1000)));
        sort($ids);
        $iterator = Producer::getIteratorWithIdsOrder($iterator, $ids);
        foreach ($iterator->generator() as $id => $bean) {
            $ex = $ex ?? $id;
            $this->assertTrue($id >= $ex, "{$id} >= {$ex}");
            $ex = $id;
        }
    }

    /**
     * @test
     */
    public function test_case_producer_get_iterator_with_sorted_values()
    {
        $iterator1 = Iterator\fromIterable([[1, []], [10, []]], 100);
        $iterator2 = Iterator\fromIterable([[8, []], [9, []], [20, []]]);
        $i = Producer::getIteratorWithSortedValues([$iterator1, $iterator2], null);
        $values = iterator_to_array($i->generator());
        $this->assertEquals([1, 8, 9, 10, 20], array_keys($values));
    }
}
