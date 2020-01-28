<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

class AbstractBean
{
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

    /** @var string */
    private const METHOD_NOT_FOUND = 'METHOD_NOT_FOUND: %s';

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
     * @return ?int
     */
    public function getId(): ?int
    {
        return $this->_id;
    }

    /**
     * @param ?int $id
     */
    public function setId(?int $id)
    {
        $this->_id = $id;
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
    public function getValues(): array
    {
        $values = [];
        foreach ($this->_fields as $name => $field) {
            $values[$name] = $field->getValue();
        }
        return $values;
    }

    /**
     * @param array $values
     */
    public function setValues(array $values)
    {
        foreach ($values as $key => $value) {
            $this->$key = $value;
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
        if ($this->_fields[$name]->setValue($value)) {
            $this->_changed[] = $name;
        }
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
        if (!isset($this->_fields[$name])) {
            throw new RuntimeException(sprintf(self::ERROR_INVALID_FIELD, $name));
        }
    }

    /**
     * @param string $name
     * @param array  $arguments
     *
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        if (substr($name, -4) === 'Sync') {
            $name = substr($name, 0, -4);
            return Promise\wait($this->$name(...$arguments));
        }
        throw new RuntimeException(sprintf(self::METHOD_NOT_FOUND, $name));
    }
}
