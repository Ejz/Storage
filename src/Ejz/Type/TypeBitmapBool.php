<?php

namespace Ejz\Type;

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
        return $value === null ? null : ($value ? '1' : '0');
    }
}
