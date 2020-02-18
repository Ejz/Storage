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
        $repository = $repository ?? $this->getByAlias($name);
        if ($repository !== null) {
            $repository->setContext($this, 'repositoryPool');
        }
        return $repository;
    }

    /**
     * @param string $name
     *
     * @return ?NameInterface
     */
    public function getByAlias(string $name): ?NameInterface
    {
        $normalize = function ($name) {
            $name = strtolower($name);
            return preg_replace('~[^a-z0-9]~', '', $name);
        };
        foreach ($this->pool as $name => $repository) {
            $avail = $repository->getAliases();
            $avail = array_map($normalize, $avail);
            if (array_search($normalize($name), $avail) !== false) {
                return $repository;
            }
        }
        return null;
    }
}
