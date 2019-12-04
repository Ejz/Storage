<?php

namespace Ejz;

use Generator;
use Amp\Loop;
use Amp\Promise;
use Amp\Producer;
use RuntimeException;

class DatabasePool
{
    const NO_SUCH_DB_ERROR = 'NO_SUCH_DB_ERROR: %s';

    /** @var DatabaseInterface[] */
    private $dbs;

    /**
     * @param DatabaseInterface[] $dbs
     */
    public function __construct(array $dbs)
    {
        $this->dbs = $dbs;
    }

    /**
     * @param string $db
     *
     * @return DatabaseInterface
     */
    public function db(string $db): DatabaseInterface
    {
        if (!isset($this->dbs[$db])) {
            throw new RuntimeException(sprintf(self::NO_SUCH_DB_ERROR, $db));
        }
        return $this->dbs[$db];
    }

    /**
     * @param array $filter
     *
     * @return DatabasePool
     */
    public function dbs(array $filter): DatabasePool
    {
        return new self(array_filter($this->dbs, function ($key) use ($filter) {
            return in_array($key, $filter);
        }, ARRAY_FILTER_USE_KEY));
    }

    /**
     * @return array
     */
    public function names(): array
    {
        return array_map('strval', array_keys($this->dbs));
    }

    /**
     * @param callable $function
     *
     * @return array
     */
    public function forEach(callable $function): array
    {
        $ret = [];
        foreach ($this->dbs as $key => $db) {
            $ret[$key] = $function($db);
        }
        return $ret;
    }

    /**
     * @param string $call
     * @param array  $arguments
     *
     * @return Promise[]
     */
    public function __call(string $call, array $arguments): array
    {
        $promises = [];
        foreach ($this->dbs as $key => $db) {
            $promises[$key] = $db->$call(...$arguments);
        }
        return $promises;
    }
}
