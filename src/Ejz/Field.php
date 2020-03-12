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
     * @param string        $name
     * @param ?AbstractType $type  (optional)
     * @param mixed         $value (optional)
     */
    public function __construct(string $name, ?AbstractType $type = null, $value = null)
    {
        $this->setName($name);
        $this->type = $type ?? DatabaseType::default(true);
        $this->setValue($value);
        $this->slave = false;
    }

    /**
     * @param mixed $value
     */
    public function importValue($value)
    {
        $this->value = $this->type->importValue($value);
    }

    /**
     * @return mixed
     */
    public function exportValue()
    {
        return $this->type->exportValue($this->value);
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @param mixed $value
     */
    public function setValue($value)
    {
        $this->value = $this->type->hydrateValue($value);
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
