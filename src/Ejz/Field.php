<?php

namespace Ejz;

class Field implements NameInterface
{
    use NameTrait;

    /** @var AbstractType */
    private $type;

    /** @var mixed */
    private $value;

    /** @var bool */
    private $slave;

    /**
     * @param string       $name
     * @param AbstractType $type
     * @param mixed        $value (optional)
     */
    public function __construct(string $name, AbstractType $type, $value = null)
    {
        $this->setName($name);
        $this->type = $type;
        $this->setValue($value);
        $this->slave = false;
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @return mixed
     */
    public function exportValue()
    {
        return $this->type->exportValue($this->getValue());
    }

    /**
     * @param mixed $value
     */
    public function setValue($value)
    {
        $this->value = $this->type->setValue($value);
    }

    /**
     * @param mixed $value
     */
    public function importValue($value)
    {
        $this->value = $this->type->importValue($value);
    }

    /**
     * @return AbstractType
     */
    public function getType(): AbstractType
    {
        return $this->type;
    }

    /**
     * @param ?bool $slave (optional)
     *
     * @return bool
     */
    public function isSlave(?bool $slave = null): bool
    {
        $this->slave = $slave ?? $this->slave;
        return $this->slave;
    }
}
