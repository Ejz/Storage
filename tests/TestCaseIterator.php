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
        $values = iterator_to_array($iterator, !!mt_rand(0, 1));
        $this->assertEquals([1, 2, 3], $values);
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
        $iterator1 = [1, 3, 6];
        $iterator2 = [2];
        $iterator3 = [100];
        $iterator = Iterator::merge([
            new Iterator($iterator1),
            new Iterator($iterator2),
            new Iterator($iterator3),
        ], function ($a, $b) {
            return $a - $b;
        });
        $values = iterator_to_array($iterator, !!mt_rand(0, 1));
        $this->assertTrue($values === [1, 2, 3, 6, 100]);
    }

    /**
     * @test
     */
    public function test_case_iterator_offset()
    {
        $iterator = [1, 2, 3];
        $values = iterator_to_array(Iterator::offset(new Iterator($iterator), 0));
        $this->assertEquals([1, 2, 3], $values);
        $values = iterator_to_array(Iterator::offset(new Iterator($iterator), 1));
        $this->assertEquals([2, 3], $values);
        $values = iterator_to_array(Iterator::offset(new Iterator($iterator), 10));
        $this->assertEquals([], $values);
    }

    /**
     * @test
     */
    public function test_case_iterator_limit()
    {
        $iterator = [1, 2, 3];
        $values = iterator_to_array(Iterator::limit(new Iterator($iterator), 0));
        $this->assertEquals([], $values);
        $values = iterator_to_array(Iterator::limit(new Iterator($iterator), 1));
        $this->assertEquals([1], $values);
        $values = iterator_to_array(Iterator::limit(new Iterator($iterator), 10));
        $this->assertEquals([1, 2, 3], $values);
    }

    /**
     * @test
     */
    public function test_case_iterator_offset_limit()
    {
        $iterator = [1, 2, 3];
        $values = iterator_to_array(Iterator::offsetLimit(new Iterator($iterator), 0, 1));
        $this->assertEquals([1], $values);
        $values = iterator_to_array(Iterator::offsetLimit(new Iterator($iterator), 1, 0));
        $this->assertEquals([], $values);
        $values = iterator_to_array(Iterator::offsetLimit(new Iterator($iterator), 0, 10));
        $this->assertEquals([1, 2, 3], $values);
        $values = iterator_to_array(Iterator::offsetLimit(new Iterator($iterator), 10, 0));
        $this->assertEquals([], $values);
    }

    /**
     * @test
     */
    public function test_case_iterator_emitter()
    {
        $emitter = new \Amp\Emitter();
        $promise = \Amp\call(function ($emitter) {
            foreach (range(1, 10) as $i) {
                yield $emitter->emit($i);
            }
        }, $emitter);
        $promise->onResolve(function () use (&$emitter) {
            $emitter->complete();
        });
        $iterator = new Iterator($emitter->iterate());
        $this->assertTrue(iterator_to_array($iterator) === range(1, 10));
    }

    /**
     * @test
     */
    public function test_case_iterator_pair()
    {
        $it1 = new Iterator([1, 2]);
        $it2 = new Iterator([3, 4, 5]);
        $res = iterator_to_array(Iterator::pair(compact('it1', 'it2')));
        $this->assertEquals($res, [['it1' => 1, 'it2' => 3], ['it1' => 2, 'it2' => 4]]);
    }
}
