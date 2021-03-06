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
        if (!$object instanceof Repository) {
            return false;
        }
        $object->setContext($this, 'repositoryPool');
        return true;
    }
}
