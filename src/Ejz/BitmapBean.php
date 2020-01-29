<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

class BitmapBean extends AbstractBean
{
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
     * @return Promise
     */
    public function add(): Promise
    {
        return $this->_repository->addBean($this);
    }
}
