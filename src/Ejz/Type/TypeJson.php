<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeJson extends AbstractType
{
    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        if ($value === null) {
            return $this->nullable ? null : [];
        }
        return $value;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function import($value)
    {
        if ($value === null) {
            return $this->nullable ? null : [];
        }
        return json_decode($value, true);
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function export($value)
    {
        if ($value === null) {
            return $this->nullable ? null : json_encode([]);
        }
        return json_encode($value);
    }
}
