<?php

namespace Ejz\Type;

class TypeBool extends AbstractType
{
    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        if ($value === null) {
            return $this->nullable ? null : false;
        }
        return (bool) $value;
    }
}
