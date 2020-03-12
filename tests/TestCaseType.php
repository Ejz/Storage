<?php

namespace Tests;

use Ejz\BitmapType;

class TestCaseType extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_bitmap_type_date()
    {
        $type = BitmapType::date('2001-01-01', '2002-10-01');
        $this->assertEquals('2001-01-01', $type->hydrateValue('2001-01-01'));
        $this->assertEquals('2001-01-01', $type->hydrateValue(mt_rand()));
        $this->assertEquals('2002-01-01', $type->hydrateValue('2002-01-01 01:01:01'));
        $this->assertEquals('2001-01-01', $type->hydrateValue(null));
        $type = BitmapType::date('2001-01-01', '2002-10-01', true);
        $this->assertEquals(null, $type->hydrateValue(null));
    }
}
