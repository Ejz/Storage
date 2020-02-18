<?php

namespace Ejz;

class DatabasePool extends AbstractPool
{
    /**
     * @param mixed $object
     *
     * @return bool
     */
    protected function checkObject($object): bool
    {
        $_ = $object instanceof DatabaseInterface;
        return $_ && parent::checkObject($object);
    }
}
