<?php

namespace Ejz;

abstract class AbstractType implements NameInterface
{
    use NameTrait;

    /** @var array */
    protected $options;

    /**
     * @param string $name
     * @param array  $options (optional)
     */
    public function __construct(string $name, array $options = [])
    {
        $this->setName($name);
        $this->options = $options;
    }

    /**
     * @return array
     */
    public function getOptions(): array
    {
        return $this->options;
    }

    /**
     * @return bool
     */
    public function isNullable(): bool
    {
        return $this->options['nullable'] ?? false;
    }

    /**
     * @return bool
     */
    public function isBinary(): bool
    {
        return $this->options['binary'] ?? false;
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

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    public function importValue($value)
    {
        return $this->setValue($value);
    }

    /**
     * @param mixed $value
     *
     * @return mixed
     */
    abstract public function setValue($value);
}
