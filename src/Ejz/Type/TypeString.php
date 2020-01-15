<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeString extends AbstractType
{
    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        if ($value === null) {
            return $this->nullable ? null : '';
        }
        return (string) $value;
    }
}
