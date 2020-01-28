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

    
    

    // private const ERROR_DELETE_WITHOUT_ID = 'ERROR_DELETE_WITHOUT_ID';
    // private const ERROR_REID_WITHOUT_ID = 'ERROR_REID_WITHOUT_ID';
    // /* -- -- -- */

    /**
     * @return string
     */
    public function getPk(): string
    {
        return $this->_repository->getDatabasePk();
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
     * @param bool $force (optional)
     *
     * @return Promise
     */
    public function update(bool $force = false): Promise
    {
        if ($this->_id === null) {
            throw new RuntimeException(self::ERROR_UPDATE_WITHOUT_ID);
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
            throw new RuntimeException(self::ERROR_DELETE_WITHOUT_ID);
        }
        return \Amp\call(function () {
            $promise = $this->_repository->delete([$this->_id]);
            return (bool) yield $promise;
        });
    }

    

    // /**
    //  * @param int $id
    //  *
    //  * @return Promise
    //  */
    // public function reid(int $id): Promise
    // {
    //     if ($this->_id === null) {
    //         $message = self::ERROR_REID_WITHOUT_ID;
    //         throw new RuntimeException($message);
    //     }
    //     return \Amp\call(function ($id) {
    //         $result = yield $this->_repository->reid($this->_id, $id);
    //         $this->setId($id);
    //         return $result;
    //     }, $id);
    // }
}
