<?php

namespace Ejz;

use Closure;

class AbstractType
{
    /** @var bool */
    protected $nullable;

    /**
     * @param bool $nullable (optional)
     */
    public function __construct(bool $nullable = false)
    {
        $this->nullable = $nullable;
    }

    /**
     * @return bool
     */
    public function isNullable(): bool
    {
        return $this->nullable;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function set($value)
    {
        return $value;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function import($value)
    {
        return $value;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function export($value)
    {
        return $value;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function serialize($value): string
    {
        return serialize($value);
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return preg_replace('~^.*?type(\w+)$~i', '$1', get_class($this));
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->getName();
    }

    /**
     * @return string
     */
    public function getSelectString(): string
    {
        return '%s';
    }

    /**
     * @return string
     */
    public function getInsertString(): string
    {
        return '?';
    }

    /**
     * @return string
     */
    public function getUpdateString(): string
    {
        return '?';
    }
}
