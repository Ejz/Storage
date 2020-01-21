<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

class Bean
{
    /** @var Repository */
    protected $_repository;

    /** @var ?int */
    protected $_id;

    /** @var array */
    protected $_fields;

    /** @var array */
    protected $_changed;

    /* -- -- -- */
    private const ERROR_INVALID_FIELD = 'ERROR_INVALID_FIELD: %s';
    private const ERROR_INSERT_WITH_ID = 'ERROR_INSERT_WITH_ID';
    private const ERROR_UPDATE_WITHOUT_ID = 'ERROR_UPDATE_WITHOUT_ID';
    private const ERROR_DELETE_WITHOUT_ID = 'ERROR_DELETE_WITHOUT_ID';
    private const ERROR_REID_WITHOUT_ID = 'ERROR_REID_WITHOUT_ID';
    /* -- -- -- */

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
     * @return string
     */
    public function getPk(): string
    {
        return $this->_repository->getPk();
    }

    /**
     * @param string $name
     * @param mixed  $value
     */
    public function __set(string $name, $value)
    {
        if ($name === 'id') {
            $this->setId($value);
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
            return $this->getId();
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
            $message = sprintf(self::ERROR_INVALID_FIELD, $name);
            throw new RuntimeException($message);
        }
    }

    /**
     * @param bool $force (optional)
     *
     * @return Promise
     */
    public function update(bool $force = false): Promise
    {
        if ($this->_id === null) {
            $message = self::ERROR_UPDATE_WITHOUT_ID;
            throw new RuntimeException($message);
        }
        return \Amp\call(function ($force) {
            $changed = array_flip($this->_changed);
            if (!$changed && !$force) {
                return false;
            }
            $fields = [];
            foreach ($this->_fields as $name => $field) {
                if ($force || isset($changed[$name])) {
                    $fields[] = $field;
                }
            }
            $promise = $this->_repository->update([$this->_id], $fields);
            $ret = (bool) yield $promise;
            $this->_changed = [];
            return $ret;
        }, $force);
    }

    /**
     * @return Promise
     */
    public function delete(): Promise
    {
        if ($this->_id === null) {
            $message = self::ERROR_DELETE_WITHOUT_ID;
            throw new RuntimeException($message);
        }
        return \Amp\call(function () {
            $promise = $this->_repository->delete([$this->_id]);
            return (bool) yield $promise;
        });
    }

    /**
     * @return Promise
     */
    public function insert(): Promise
    {
        if ($this->_id !== null) {
            $message = self::ERROR_INSERT_WITH_ID;
            throw new RuntimeException($message);
        }
        return \Amp\call(function () {
            $id = yield $this->_repository->insertBean($this);
            $this->setId($id);
            return $id;
        });
    }

    /**
     * @param int $id
     *
     * @return Promise
     */
    public function reid(int $id): Promise
    {
        if ($this->_id === null) {
            $message = self::ERROR_REID_WITHOUT_ID;
            throw new RuntimeException($message);
        }
        return \Amp\call(function ($id) {
            $result = yield $this->_repository->reid($this->_id, $id);
            $this->setId($id);
            return $result;
        }, $id);
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function deleteSync(...$args)
    {
        return Promise\wait($this->delete(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function updateSync(...$args)
    {
        return Promise\wait($this->update(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function insertSync(...$args)
    {
        return Promise\wait($this->insert(...$args));
    }

    /**
     * @param array ...$args
     *
     * @return mixed
     */
    public function reidSync(...$args)
    {
        return Promise\wait($this->reid(...$args));
    }
}
