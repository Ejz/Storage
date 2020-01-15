<?php

namespace Ejz;

class Index
{
    /** @var string */
    private $name;

    /** @var array */
    private $childFields;

    /** @var string */
    private $parentTable;

    /** @var array */
    private $parentFields;

    /**
     * @param string $name
     * @param array  $childFields
     * @param string $parentTable
     * @param array  $parentFields
     */
    public function __construct(
        string $name,
        array $childFields,
        string $parentTable,
        array $parentFields
    ) {
        $this->name = $name;
        $this->childFields = $childFields;
        $this->parentTable = $parentTable;
        $this->parentFields = $parentFields;
    }

    /**
     * @return array
     */
    public function getChildFields(): array
    {
        return $this->childFields;
    }

    /**
     * @return array
     */
    public function getParentFields(): array
    {
        return $this->parentFields;
    }

    /**
     * @return string
     */
    public function getParentTable(): string
    {
        return $this->parentTable;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->getName();
    }
}
