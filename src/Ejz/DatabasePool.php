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
        $dbs = [];
        foreach ($this->dbs as $key => $db) {
            if (in_array($key, $filter, true)) {
                $dbs[$key] = $db;
            }
        }
        return new self($dbs);
    }

    /**
     * @return array
     */
    public function names(): array
    {
        return array_keys($this->dbs);
    }

    /**
     * @param string $call
     * @param array  $arguments
     *
     * @return array|Promise
     */
    public function __call(string $call, array $arguments)
    {
        $is_async = stripos($call, 'async') !== false;
        if (!$is_async) {
            $call .= 'Async';
        }
        $promises = [];
        foreach ($this->dbs as $key => $db) {
            $promises[$key] = $db->$call(...$arguments);
        }
        $promise = \Amp\Promise\all($promises);
        if ($is_async) {
            return $promise;
        }
        Loop::run(function () use ($promise, &$ret) {
            $ret = yield $promise;
        });
        return $ret;
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate(string $table, array $params = []): Generator
    {
        $ret = null;
        $iterator = function () use ($table, $params, &$ret) {
            static $iterator;
            if ($iterator === null) {
                $iterator = $this->iterateAsync($table, $params);
            }
            $ret = null;
            if (yield $iterator->advance()) {
                $ret = $iterator->getCurrent();
            }
        };
        do {
            \Amp\Loop::run($iterator);
            if ($ret === null) {
                break;
            }
            yield $ret;
        } while (true);
    }

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Producer
     */
    public function iterateAsync(string $table, array $params = []): Producer
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
