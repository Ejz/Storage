<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeBitmapBool extends TypeBool
{
    /**
     */
    public function __construct()
    {
        parent::__construct(true);
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function export($value)
    {
        return $value ? '1' : '0';
    }
}
