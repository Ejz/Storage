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
        $_ = $object instanceof BitmapInterface;
        return $_ && parent::checkObject($object);
    }
}
