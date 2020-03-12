<?php

namespace Ejz;

class BitmapPool extends AbstractPool
{
    /**
     * @param mixed $object
     *
     * @return bool
     */
    protected function checkObject($object): bool
    {
        return $object instanceof BitmapInterface;
    }
}
