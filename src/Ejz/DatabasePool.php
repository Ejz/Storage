<?php

namespace Ejz;

use RuntimeException;

class DatabasePool
{
    private const NO_SUCH_DB_ERROR = 'NO_SUCH_DB_ERROR: %s';
    private const INVALID_DB_NAMES = 'INVALID_DB_NAMES';
    private const EMPTY_POOL_ERROR = 'EMPTY_POOL_ERROR';

    /** @var array */
    private $dbs;

    /**
     * @param array $dbs
     */
    public function __construct(array $dbs)
    {
        $this->dbs = [];
        foreach ($dbs as $db) {
            $this->dbs[$db->getName()] = $db;
        }
        if (!$this->dbs) {
            throw new RuntimeException(self::EMPTY_POOL_ERROR);
        }
        if (count($this->dbs) != count($dbs)) {
            throw new RuntimeException(self::INVALID_DB_NAMES);
        }
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
     * @param mixed $filter
     *
     * @return DatabasePool
     */
    public function filter($filter): DatabasePool
    {
        if (!is_callable($filter)) {
            $f = (array) $filter;
            $filter = function ($name) use ($f) {
                return in_array($name, $f, true);
            };
        }
        $names = $this->names();
        return new self(array_filter($this->dbs, function ($key) use ($filter, $names) {
            return $filter($key, $names);
        }, ARRAY_FILTER_USE_KEY));
    }

    /**
     * @return DatabaseInterface
     */
    public function random(): DatabaseInterface
    {
        return $this->dbs[array_rand($this->dbs)];
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
     * @return array
     */
    public function __call(string $call, array $arguments): array
    {
        $result = [];
        foreach ($this->dbs as $key => $db) {
            $result[$key] = $db->$call(...$arguments);
        }
        return $result;
    }
}
