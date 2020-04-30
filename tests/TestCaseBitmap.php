<?php

namespace Tests;

use Ejz\Field;
use Ejz\BitmapType;
use Ejz\Iterator;

class TestCaseBitmap extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_bitmap_create_drop()
    {
        $bm = $this->bitmapPool->random();
        $bm->createSync('tt');
        $this->assertTrue($bm->indexesSync() === ['tt']);
        $this->assertTrue($bm->indexExistsSync('tt'));
        $bm->dropSync('tt');
        $this->assertTrue($bm->indexesSync() === []);
        $this->assertTrue(!$bm->indexExistsSync('tt'));
    }

    /**
     * @test
     */
    public function test_case_bitmap_add_search()
    {
        $bm = $this->bitmapPool->random();
        $bm->createSync('tt');
        foreach (range(1, 1000) as $id) {
            $bm->addSync('tt', $id);
        }
        $keys = [];
        foreach ($bm->search('tt') as $id) {
            $keys[] = $id;
        }
        $this->assertEquals(range(1, 1000), $keys);
        $keys = [];
        foreach ($bm->search('tt', ['min' => 70]) as $id) {
            $keys[] = $id;
        }
        $this->assertEquals(range(70, 1000), $keys);
        //
        $keys = [];
        foreach (Iterator::offset($bm->search('tt', ['emitTotal' => true]), 1) as $id) {
            $keys[] = $id;
        }
        $this->assertEquals(range(1, 1000), $keys);
        $keys = [];
        foreach (Iterator::offset($bm->search('tt', ['min' => 70, 'emitTotal' => true]), 1) as $id) {
            $keys[] = $id;
        }
        $this->assertEquals(range(70, 1000), $keys);
    }

    /**
     * @test
     */
    public function test_case_bitmap_multi_iterators()
    {
        $bm = $this->bitmapPool->random();
        $bm->createSync('t1');
        foreach (range(1, 1000) as $id) {
            $bm->addSync('t1', $id);
        }
        [$it1, $it2] = $bm->search('t1', [['min' => 850], ['min' => 100]]);
        $this->assertEquals(range(850, 1000), iterator_to_array($it1));
        $this->assertEquals(range(100, 1000), iterator_to_array($it2));
    }

    /**
     * @test
     */
    public function test_case_bitmap_search_with_sort()
    {
        $bm = $this->bitmapPool->random();
        $type = BitmapType::INTEGER();
        $bm->createSync('t1', [new Field('int', $type)]);
        $values = [];
        foreach (range(1, 100) as $id) {
            $_ = mt_rand(1, 100);
            $bm->addSync('t1', $id, [new Field('int', $type, $_)]);
            $values[] = [$id, $_];
        }
        $values_asc = $values;
        $values_desc = $values;
        usort($values_asc, function ($a, $b) {
            return $a[1] == $b[1] ? $a[0] - $b[0] : $a[1] - $b[1];
        });
        usort($values_desc, function ($a, $b) {
            return $a[1] == $b[1] ? $a[0] - $b[0] : $b[1] - $a[1];
        });
        $values_asc = array_map(function ($value) {
            return $value[0];
        }, $values_asc);
        $values_desc = array_map(function ($value) {
            return $value[0];
        }, $values_desc);
        [$it1, $it2, $it3, $it4, $it5] = $bm->search('t1', [
            ['sortby' => 'int', 'asc' => true],
            ['sortby' => 'int', 'asc' => false],
            ['sortby' => 'int', 'asc' => true, 'min' => 50],
            ['sortby' => 'int', 'asc' => false, 'min' => 500],
            ['sortby' => 'int', 'asc' => true, 'min' => 500, 'max' => 600],
        ]);
        $this->assertEquals($values_asc, iterator_to_array($it1));
        $this->assertEquals($values_desc, iterator_to_array($it2));
        $this->assertEquals(array_values(array_filter($values_asc, function ($id) {
            return $id >= 50;
        })), iterator_to_array($it3));
        $this->assertEquals(array_values(array_filter($values_desc, function ($id) {
            return $id >= 500;
        })), iterator_to_array($it4));
        $this->assertEquals(array_values(array_filter($values_desc, function ($id) {
            return $id >= 500 && $id <= 600;
        })), iterator_to_array($it5));
    }

    /**
     * @test
     */
    public function test_case_bitmap_search_foreign_keys()
    {
        $bm = $this->bitmapPool->random();
        $bm->createSync('parent');
        $options = ['references' => 'parent', 'nullable' => true];
        $field = new Field('parent_id', BitmapType::FOREIGNKEY($options));
        $bm->createSync('child', [$field]);
        $bm->addSync('parent', 1);
        $bm->addSync('parent', 2);
        $field->setValue(1);
        $bm->addSync('child', 1, [$field]);
        $bm->addSync('child', 2, [$field]);
        $field->setValue(2);
        $bm->addSync('child', 3, [$field]);
        $it = $bm->search('child', ['foreignKeys' => 'parent_id']);
        foreach ($it as $row) {
            $this->assertTrue(isset($row['id']));
        }
        $it = $bm->search('child', ['forceForeignKeyFormat' => true]);
        foreach ($it as $row) {
            $this->assertTrue(isset($row['id']));
        }
    }

    /**
     * @test
     */
    public function test_case_bitmap_type_date()
    {
        $type = BitmapType::DATE(['min' => '2001-01-01', 'max' => '2002-10-01']);
        $this->assertEquals('2001-01-01', $type->importValue('2001-01-01'));
        $this->assertEquals(null, $type->importValue(mt_rand()));
        $this->assertEquals('2002-01-01', $type->importValue('2002-01-01 01:01:01'));
        $this->assertEquals(null, $type->importValue(null));
    }
}
