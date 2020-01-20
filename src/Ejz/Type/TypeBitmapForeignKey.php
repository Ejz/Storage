<?php

namespace Ejz\Type;

use Ejz\AbstractType;

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
}
