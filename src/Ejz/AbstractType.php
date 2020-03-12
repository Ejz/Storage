<?php

namespace Ejz;

class AbstractType implements NameInterface
{
    use NameTrait;

    /** @var bool */
    protected $nullable;

    /** @var bool */
    protected $binary;

    /**
     * @param string $name
     * @param bool   $nullable
     */
    public function __construct(string $name, bool $nullable)
    {
        $this->setName($name);
        $this->nullable = $nullable;
        $this->binary = false;
    }

    /**
     * @return bool
     */
    public function isNullable(): bool
    {
        return $this->nullable;
    }

    /**
     * @return bool
     */
    public function isBinary(): bool
    {
        return $this->binary;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function hydrateValue($value)
    {
        if ($value === null) {
            return $this->nullable ? null : '';
        }
        return $value;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function importValue($value)
    {
        return $this->hydrateValue($value);
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function exportValue($value)
    {
        return $value;
    }
}
