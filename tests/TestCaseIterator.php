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

    /**
     * @test
     */
    public function test_case_iterator_offset()
    {
        $iterator = [[1, ''], [2, ''], [3, '']];
        $values = iterator_to_array(Iterator::offset(new Iterator($iterator), 0));
        $values = array_keys($values);
        $this->assertEquals([1, 2, 3], $values);
        $values = iterator_to_array(Iterator::offset(new Iterator($iterator), 1));
        $values = array_keys($values);
        $this->assertEquals([2, 3], $values);
        $values = iterator_to_array(Iterator::offset(new Iterator($iterator), 10));
        $values = array_keys($values);
        $this->assertEquals([], $values);
    }
}
