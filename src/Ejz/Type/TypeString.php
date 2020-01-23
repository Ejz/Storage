<?php

namespace Ejz\Type;

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
