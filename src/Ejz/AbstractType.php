<?php

namespace Ejz;

use Closure;

class AbstractType
{
    /** @var bool */
    private $nullable;

    /**
     * @param bool $nullable (optional)
     */
    public function __construct(bool $nullable = false)
    {
        $this->nullable = $nullable;
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function cast($value)
    {
        return $value;
    }

    /**
     * @return bool
     */
    public function isNullable(): bool
    {
        return $this->nullable;
    }

    /**
     * @return Closure
     */
    public function getSelectStringHandler(): Closure
    {
        return function () {
            return '%s';
        };
    }

    /**
     * @return Closure
     */
    public function getSelectValueHandler(): Closure
    {
        return function ($value) {
            return $value;
        };
    }

    /**
     * @return Closure
     */
    public function getInsertStringHandler(): Closure
    {
        return function () {
            return '%s';
        };
    }

    /**
     * @return Closure
     */
    public function getInsertValueHandler(): Closure
    {
        return function ($value) {
            return $value;
        };
    }

    /**
     * @return Closure
     */
    public function getUpdateStringHandler(): Closure
    {
        return function () {
            return '%s';
        };
    }

    /**
     * @return Closure
     */
    public function getUpdateValueHandler(): Closure
    {
        return function ($value) {
            return $value;
        };
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return preg_replace('~^.*([A-Z][a-z]+)$~', '$1', get_class($this));
    }

    /**
     * @return string
     */
    public function __toString(): string
    {
        return $this->getName();
    }
}
