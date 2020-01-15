<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeFloat extends AbstractType
{
    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        if ($value === null) {
            return $this->nullable ? null : 0;
        }
        return (float) $value;
    }
}
