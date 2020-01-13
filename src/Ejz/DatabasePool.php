<?php

namespace Ejz;

use RuntimeException;

class DatabasePool
{
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
    }

    /**
     * @param string $db
     *
     * @return ?DatabaseInterface
     */
    public function db(string $db): ?DatabaseInterface
    {
        return $this->dbs[$db] ?? null;
    }

    /**
     * @param mixed $filter
     *
     * @return DatabasePool
     */
    public function filter($filter): DatabasePool
    {
        $names = $this->names();
        $dbs = [];
        foreach ($this->dbs as $name => $db) {
            if (
                (is_callable($filter) && $filter($name, $names)) ||
                in_array($name, (array) $filter)
            ) {
                $dbs[] = $db;
            }
        }
        return new self($dbs);
    }

    /**
     * @return ?DatabaseInterface
     */
    public function random(): ?DatabaseInterface
    {
        @ $key = array_rand($this->dbs);
        return $key === null ? null : $this->dbs[$key];
    }

    /**
     * @return array
     */
    public function names(): array
    {
        return array_map('strval', array_keys($this->dbs));
    }

    /**
     * @return int
     */
    public function size(): int
    {
        return count($this->dbs);
    }

    /**
     * @param callable $function
     *
     * @return array
     */
    public function each(callable $function): array
    {
        return array_map(function ($db) use ($function) {
            return $function($db);
        }, $this->dbs);
    }

    /**
     * @param string $call
     * @param array  $arguments
     *
     * @return array
     */
    public function __call(string $call, array $arguments): array
    {
        return $this->each(function ($db) use ($call, $arguments) {
            return $db->$call(...$arguments);
        });
    }
}
