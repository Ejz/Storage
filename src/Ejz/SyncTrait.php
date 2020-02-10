<?php

namespace Ejz;

use Amp\Promise;
use RuntimeException;

trait SyncTrait
{
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
        throw new RuntimeException(sprintf('ERROR_METHOD_NOT_FOUND: %s', $name));
    }
}