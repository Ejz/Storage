<?php

namespace Ejz;

use Generator;
use Amp\Loop;
use Amp\Promise;
use Amp\Producer;
use RuntimeException;

class DatabasePool
{
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
            throw new RuntimeException();
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
            return in_array($key, $filter, true);
        }, ARRAY_FILTER_USE_KEY));
    }

    /**
     * @return array
     */
    public function names(): array
    {
        return array_keys($this->dbs);
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

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate2(string $table, array $params = []): Generator
    {
        $producer = $this->iterateAsync($table, $params);
        $iterator = function ($producer) {
            if (yield $producer->advance()) {
                return $producer->getCurrent();
            }
        };
        while ($yield = \Amp\call($iterator, $producer)) {
            yield $yield;
        }
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Producer
     */
    public function iterateAsync2(string $table, array $params = []): Producer
    {
        $emit = function (callable $emit) use ($table, $params) {
            $iterators = [];
            foreach ($this->dbs as $key => $db) {
                $iterators[$key] = $db->iterateAsync($table, $params);
            }
            $values = [];
            while ($iterators) {
                $keys = array_keys($iterators);
                foreach ($keys as $key) {
                    if (isset($values[$key])) {
                        continue;
                    }
                    if (yield $iterators[$key]->advance()) {
                        $values[$key] = $iterators[$key]->getCurrent();
                    } else {
                        unset($iterators[$key]);
                    }
                }
                uasort($values, function ($a, $b) {
                    return $a[0] - $b[0];
                });
                if (!$values) {
                    break;
                }
                $key = array_keys($values)[0];
                yield $emit($values[$key]);
                unset($values[$key]);
            }
        };
        return new Producer($emit);
    }
}
