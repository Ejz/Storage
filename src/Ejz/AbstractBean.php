<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

class AbstractBean
{
    use SyncTrait;

    /** @var Repository */
    protected $_repository;

    /** @var ?int */
    protected $_id;

    /** @var array */
    protected $_fields;

    /** @var array */
    protected $_changed;

    /** @var string */
    private const ERROR_INVALID_FIELD = 'ERROR_INVALID_FIELD: %s';

    /**
     * @param Repository $repository
     * @param ?int       $id
     * @param array      $fields
     */
    public function __construct(Repository $repository, ?int $id, array $fields)
    {
        $this->_repository = $repository;
        $this->_id = $id;
        $this->_fields = $fields;
        $this->_changed = [];
    }

    /**
     * @return Repository
     */
    public function getRepository(): Repository
    {
        return $this->_repository;
    }

    /**
     * @return array
     */
    public function getFields(): array
    {
        return $this->_fields;
    }

    /**
     * @return array
     */
    public function getSlaveFields(): array
    {
        return array_filter($this->_fields, function ($field) {
            return $field->isSlave();
        });
    }

    /**
     * @return array
     */
    public function getValues(): array
    {
        $values = [];
        foreach ($this->getFields() as $name => $field) {
            $values[$name] = $field->getValue();
        }
        return $values;
    }

    /**
     * @return array
     */
    public function getSlaveValues(): array
    {
        $values = [];
        foreach ($this->getSlaveFields() as $name => $field) {
            $values[$name] = $field->getValue();
        }
        return $values;
    }

    /**
     * @param array $values
     */
    public function setValues(array $values)
    {
        foreach ($values as $name => $value) {
            $this->checkField($name);
            $this->_fields[$name]->setValue($value);
            $this->_changed[$name] = true;
        }
    }

    /**
     * @param string $name
     * @param mixed  $value
     */
    public function __set(string $name, $value)
    {
        if ($name === 'id') {
            $this->_id = $value;
            return;
        }
        $this->checkField($name);
        $this->_fields[$name]->setValue($value);
        $this->_changed[$name] = true;
    }

    /**
     * @param string $name
     *
     * @return mixed
     */
    public function __get(string $name)
    {
        if ($name === 'id') {
            return $this->_id;
        }
        $this->checkField($name);
        return $this->_fields[$name]->getValue();
    }

    /**
     * @param string $name
     */
    private function checkField(string $name)
    {
        if (!array_key_exists($name, $this->_fields)) {
            throw new RuntimeException(sprintf(self::ERROR_INVALID_FIELD, $name));
        }
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public function __isset(string $name): bool
    {
        if ($name === 'id') {
            return $this->_id !== null;
        }
        return array_key_exists($name, $this->_fields);
    }
}
