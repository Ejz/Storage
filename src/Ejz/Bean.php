<?php

namespace Ejz;

class Bean
{
    /**
     * @param Repository $repository
     * @param int        $id
     * @param array      $row
     */
    public function __construct(Repository $repository, int $id, array $row)
    {
        $this->repository = $repository;
        $this->id = $id;
        $this->row = $row;
        $this->changed = [];
    }

    /**
     * @return Repository
     */
    public function getRepository(): Repository
    {
        return $this->repository;
    }

    /**
     * @return int
     */
    public function getId(): int
    {
        return $this->id;
    }

    /**
     * @return string
     */
    public function getPk(): string
    {
        return $this->repository->getPk();
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->repository->getName();
    }

    public function __set($name, $value)
    {

    }

    public function update($force = false) {

    }

    public function delete() {
        
    }
}
