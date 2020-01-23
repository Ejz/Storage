<?php

namespace Ejz\Type;

class TypeCompressedBinary extends TypeBinary
{
    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function import($value)
    {
        return gzinflate(parent::import($value));
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function export($value)
    {
        return parent::export(gzdeflate($value));
    }
}
