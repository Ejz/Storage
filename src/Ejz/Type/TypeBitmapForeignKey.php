<?php

namespace Ejz\Type;

class TypeBitmapForeignKey extends TypeInt
{
    /** @var string */
    private $parentTable;

    /**
     * @param string $parentTable
     */
    public function __construct(string $parentTable)
    {
        $this->parentTable = $parentTable;
        parent::__construct(true);
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
    public function getParentIndex(): string
    {
        return $this->parentTable;
    }

    /**
     * @return string
     */
    public function getParent(): string
    {
        return $this->parentTable;
    }
}
