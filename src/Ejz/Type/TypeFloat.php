<?php

namespace Ejz\Type;

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
