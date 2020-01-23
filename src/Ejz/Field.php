<?php

namespace Ejz;

use Ejz\Type\AbstractType;

class Field
{
    /** @var string */
    private $name;

    /** @var AbstractType */
    private $type;

    /** @var string */
    private $alias;

    /** @var mixed */
    private $value;

    /**
     * @param string        $name
     * @param ?AbstractType $type  (optional)
     * @param ?string       $alias (optional)
     */
    public function __construct(string $name, ?AbstractType $type = null, ?string $alias = null)
    {
        $this->name = $name;
        $this->type = $type ?? Type::default(true);
        $this->alias = $alias ?? $this->name;
        $this->setValue(null);
    }

    /**
     * @param string $quote
     *
     * @return string
     */
    public function getSelectString(string $quote): string
    {
        $selectString = $this->type->getSelectString();
        return str_replace('%s', $quote . $this->name . $quote, $selectString);
    }

    /**
     * @return string
     */
    public function getInsertString(): string
    {
        return $this->type->getInsertString();
    }

    /**
     * @return string
     */
    public function getUpdateString(string $quote): string
    {
        $updateString = $this->type->getUpdateString();
        return str_replace('%s', $quote . $this->name . $quote, $updateString);
    }

    /**
     * @param mixed $value
     */
    public function importValue($value)
    {
        $value = $this->type->import($value);
        $this->setValue($value);
    }

    /**
     * @return mixed
     */
    public function exportValue()
    {
        return $this->type->export($this->value);
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @return string
     */
    public function getAlias(): string
    {
        return $this->alias;
    }

    /**
     * @param mixed $value
     *
     * @return bool
     */
    public function setValue($value): bool
    {
        $old = $this->type->serialize($this->value);
        $this->value = $this->type->set($value);
        $new = $this->type->serialize($this->value);
        return $old !== $new;
    }

    /**
     * @return AbstractType
     */
    public function getType(): AbstractType
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
