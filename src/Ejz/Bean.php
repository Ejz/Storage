<?php

namespace Ejz;

class Bean
{
    /** @var Repository */
    private $repository;

    /** @var ?int */
    private $id;

    /** @var array */
    private $fields;

    /** @var array */
    private $changed;

    /**
     * @param Repository $repository
     * @param ?int       $id
     * @param array      $fields
     */
    public function __construct(Repository $repository, ?int $id, array $fields)
    {
        $this->repository = $repository;
        $this->id = $id;
        $this->fields = $fields;
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
     * @return ?int
     */
    public function getId(): ?int
    {
        return $this->id;
    }

    /**
     * @param int $id
     */
    public function setId(int $id)
    {
        $this->id = $id;
    }

    /**
     */
    public function unsetId()
    {
        $this->id = null;
    }

    /**
     * @return array
     */
    public function getFields(): array
    {
        return $this->fields;
    }

    /**
     * @return array
     */
    public function getValues(): array
    {
        $values = [];
        foreach ($this->fields as $name => $field) {
            $values[$name] = $field->getValue();
        }
        return $values;
    }

    /**
     * @return string
     */
    public function getPk(): string
    {
        return $this->repository->getPk();
    }
}
