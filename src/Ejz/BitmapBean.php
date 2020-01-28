<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

class BitmapBean extends AbstractBean
{
    /* -- -- -- */
    private const ERROR_ADD_WITHOUT_ID = 'ERROR_ADD_WITHOUT_ID';
    /* -- -- -- */

    /**
     * @param Repository $repository
     * @param int        $id
     * @param array      $fields
     */
    public function __construct(Repository $repository, int $id, array $fields)
    {
        parent::__construct($repository, $id, $fields);
    }

    /**
     */
    public function add()
    {
        if ($this->_id === null) {
            $message = self::ERROR_ADD_WITHOUT_ID;
            throw new RuntimeException($message);
        }
        return $this->_repository->bitmapAddBean($this);
    }
}
