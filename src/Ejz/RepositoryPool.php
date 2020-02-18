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

    /**
     * @param string $name
     *
     * @return ?NameInterface
     */
    public function get(string $name): ?NameInterface
    {
        $repository = parent::get($name);
        if ($repository !== null) {
            $repository->setContext($this, 'repositoryPool');
        }
        return $repository;
    }
}
