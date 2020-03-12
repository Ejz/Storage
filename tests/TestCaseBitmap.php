<?php

namespace Tests;

use Ejz\Field;
use Ejz\FieldPool;
use Ejz\WhereCondition;
use Ejz\BitmapType;

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
        foreach ($bm->search('tt') as [$id]) {
            $keys[] = $id;
        }
        $this->assertEquals(range(1, 1000), $keys);
    }

    /**
     * @test
     */
    public function test_case_bitmap_search_fk()
    {
        $bm = $this->bitmapPool->random();
        $bm->createSync('parent');
        $fields = new FieldPool([new Field('fk', BitmapType::foreignKey('parent'))]);
        $bm->createSync('child', $fields);
        $fk = new Field('fk');
        $fk->setValue(1);
        $f = new FieldPool([$fk]);
        $bm->addSync('child', 1, $f);
        $all = iterator_to_array($bm->search('child', [
            'fks' => 'fk',
        ]));
        $this->assertTrue($all === [[1, [1]]]);
    }
}
