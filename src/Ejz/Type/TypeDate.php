<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeDate extends AbstractType
{
    private const DATE_FORMAT = 'Y-m-d';

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        if ($value === null) {
            return $this->nullable ? null : date(self::DATE_FORMAT, 0);
        }
        return date(self::DATE_FORMAT, strtotime($value));
    }
}
