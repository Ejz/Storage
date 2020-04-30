<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

class DatabaseBean extends AbstractBean
{
    /** @var string */
    private const ERROR_INSERT_WITH_ID = 'ERROR_INSERT_WITH_ID';

    /** @var string */
    private const ERROR_UPDATE_WITHOUT_ID = 'ERROR_UPDATE_WITHOUT_ID';

    /** @var string */
    private const ERROR_DELETE_WITHOUT_ID = 'ERROR_DELETE_WITHOUT_ID';

    /** @var string */
    private const ERROR_REID_WITHOUT_ID = 'ERROR_REID_WITHOUT_ID';

    /**
     * @return string
     */
    public function getPrimaryKey(): string
    {
        return $this->_repository->getDatabasePrimaryKey();
    }

    /**
     * @return Promise
     */
    public function insert(): Promise
    {
        if ($this->_id !== null) {
            throw new RuntimeException(self::ERROR_INSERT_WITH_ID);
        }
        return \Amp\call(function () {
            $id = yield $this->_repository->insertBean($this);
            $this->id = $id;
            return $id;
        });
    }

    /**
     * @return Promise
     */
    public function update(): Promise
    {
        if ($this->_id === null) {
            throw new RuntimeException(self::ERROR_UPDATE_WITHOUT_ID);
        }
        return \Amp\call(function () {
            $changed = $this->_changed;
            if (!$changed) {
                return false;
            }
            $fields = [];
            foreach ($this->_fields as $name => $field) {
                if (isset($changed[$name])) {
                    $fields[] = $field;
                }
            }
            $promise = $this->_repository->update([$this->_id], $fields);
            $ret = (bool) yield $promise;
            $this->_changed = [];
            return $ret;
        });
    }

    /**
     * @return Promise
     */
    public function delete(): Promise
    {
        if ($this->_id === null) {
            throw new RuntimeException(self::ERROR_DELETE_WITHOUT_ID);
        }
        return \Amp\call(function () {
            $promise = $this->_repository->delete([$this->_id]);
            return (bool) yield $promise;
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
            throw new RuntimeException(self::ERROR_REID_WITHOUT_ID);
        }
        return \Amp\call(function ($id) {
            $result = yield $this->_repository->reid([$this->_id, $id]);
            $this->id = $id;
            return $result;
        }, $id);
    }
}
