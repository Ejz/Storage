<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeInt extends AbstractType
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
        return (int) $value;
    }
}
