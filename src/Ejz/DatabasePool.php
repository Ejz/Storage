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
        return $object instanceof DatabaseInterface;
    }
}
