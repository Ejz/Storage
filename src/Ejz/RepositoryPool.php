<?php

namespace Ejz;

class RepositoryPool extends AbstractPool
{
    /**
     * @param mixed $object
     *
     * @return bool
     */
    protected function checkObject($object): bool
    {
        $_ = $object instanceof Repository;
        return $_ && parent::checkObject($object);
    }
}
