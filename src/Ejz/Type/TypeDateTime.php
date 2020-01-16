<?php

namespace Ejz\Type;

use Ejz\AbstractType;

class TypeDateTime extends AbstractType
{
    private const DATE_TIME_FORMAT = 'Y-m-d H:i:s';

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        if ($value === null) {
            return $this->nullable ? null : date(self::DATE_TIME_FORMAT, 0);
        }
        return date(self::DATE_TIME_FORMAT, strtotime($value));
    }
}
