<?php

namespace Ejz;

class Index
{
    /** @var string */
    private $name;

    /** @var array */
    private $fields;

    /** @var string */
    private $type;

    /* -- -- -- */
    public const INDEX_TYPE_HASH = 'INDEX_TYPE_HASH';
    public const INDEX_TYPE_BTREE = 'INDEX_TYPE_BTREE';
    public const INDEX_TYPE_GIST = 'INDEX_TYPE_GIST';
    public const INDEX_TYPE_GIN = 'INDEX_TYPE_GIN';
    public const INDEX_TYPE_UNIQUE = 'INDEX_TYPE_UNIQUE';
    /* -- -- -- */

    /**
     * @param string  $name
     * @param array   $fields
     * @param ?string $type   (optional)
     */
    public function __construct(string $name, array $fields, ?string $type = null)
    {
        $this->name = $name;
        $this->fields = $fields;
        $this->type = $type ?? self::INDEX_TYPE_BTREE;
    }

    /**
     * @return array
     */
    public function getFields(): array
    {
        return $this->fields;
    }

    /**
     * @return string
     */
    public function getType(): string
    {
        return $this->type;
    }

    /**
     * @return bool
     */
    public function isUnique(): bool
    {
        return $this->type === self::INDEX_TYPE_UNIQUE;
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
