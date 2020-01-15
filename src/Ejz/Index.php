<?php

namespace Ejz;

class Index
{
    /** @var string */
    private $name;

    /** @var array */
    private $fields;

    /** @var ?string */
    private $type;

    /**
     * @param string  $name
     * @param array   $fields
     * @param ?string $type   (optional)
     */
    public function __construct(string $name, array $fields, ?string $type = null)
    {
        $this->name = $name;
        $this->fields = $fields;
        $this->type = $type;
    }

    /**
     * @return array
     */
    public function getFields(): array
    {
        return $this->fields;
    }

    /**
     * @return ?string
     */
    public function getType(): ?string
    {
        return $this->type;
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
