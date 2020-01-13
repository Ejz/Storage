<?php

namespace Ejz;

class Type
{
    /**
     * @param bool $nullable (optional)
     *
     * @return AbstractType
     */
    public static function string(bool $nullable = false): AbstractType
    {
        return new Type\TypeString($nullable);
    }
}
