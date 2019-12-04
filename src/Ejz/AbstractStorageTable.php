<?php

namespace Ejz;

use Generator;
use Error;
use Amp\Loop;
use Amp\Promise;
use Amp\Deferred;
use Amp\Producer;
use RuntimeException;

abstract class AbstractStorageTable
{
    /** @var DatabasePool */
    private $pool;

    /** @var TableDefinition */
    private $definition;

    /**
     * @param DatabasePool    $pool
     * @param TableDefinition $definition
     */
    public function __construct(
        DatabasePool $pool,
        TableDefinition $definition
    ) {
        $this->pool = $pool;
        $this->definition = $definition;
    }

    /**
     * @return Promise
     */
    public function truncateAsync(): Promise
    {
        return \Amp\call(function () {
            $table = $this->definition->getTable();
            return yield $this->pool->truncateAsync($table);
        });
    }

    /**
     * @return array
     */
    public function truncate(): array
    {
        return \Amp\Promise\wait($this->truncateAsync());
    }

    /**
     * @return Promise
     */
    public function createAsync(): Promise
    {
        return \Amp\call(function () {
            return yield $this->pool->createAsync($this->definition);
        });
    }

    /**
     *
     */
    public function create()
    {
        return \Amp\Promise\wait($this->createAsync());
    }

    /**
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function insertAsync(array $values = []): Promise
    {
        $values = $this->definition->setDefaultValues($values);
        return \Amp\call(function ($values) {
            $deferred = new Deferred();
            $pool = $this->getShards(null, $values);
            $promises = $pool->insertAsync($this->definition, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err, $res) use ($deferred) {
                if ($err) {
                    return $deferred->resolve(0);
                }
                $ids = array_values($res);
                $id = array_pop($ids);
                foreach ($ids as $_) {
                    if ($_ !== $id) {
                        return $deferred->resolve(0);
                    }
                }
                $deferred->resolve($id);
            });
            return $deferred->promise();
        }, $values);
    }

    /**
     * @param array $values (optional)
     *
     * @return int
     */
    public function insert(array $values = []): int
    {
        return \Amp\Promise\wait($this->insertAsync($values));
    }

    /**
     * @param int   $id
     * @param array $values (optional)
     *
     * @return Promise
     */
    public function updateAsync(int $id, array $values = []): Promise
    {
        $values = $this->definition->setDefaultValues($values);
        return \Amp\call(function ($id, $values) {
            $deferred = new Deferred();
            $pool = $this->getShards($id, $values);
            $promises = $pool->updateAsync($this->definition, $id, $values);
            \Amp\Promise\all($promises)->onResolve(function ($err) use ($deferred) {
                $deferred->resolve(!$err);
            });
            return $deferred->promise();
        }, $id, $values);
    }

    /**
     * @param int   $id
     * @param array $values (optional)
     *
     * @return bool
     */
    public function update(int $id, array $values = []): bool
    {
        return \Amp\Promise\wait($this->updateAsync($id, $values));
    }

    /**
     * @param int $id
     * @param mixed $fields (optional)
     *
     * @return Promise
     */
    public function getAsync(int $id, $fields = null): Promise
    {
        $values = $this->definition->setDefaultValues([]);
        return \Amp\call(function ($id, $values, $fields) {
            $db = $this->getShard($id, $values);
            return yield $db->getAsync($this->definition, $id, $fields);
        }, $id, $values, $fields);
    }

    /**
     * @param int   $id
     * @param mixed $fields (optional)
     *
     * @return array
     */
    public function get(int $id, $fields = null): array
    {
        return \Amp\Promise\wait($this->getAsync($id, $fields));
    }

    /**
     * @param array $params (optional)
     *
     * @return array
     */
    public function iterate(array $params = []): Producer
    {
        $table = $this->definition->getTable();
        $params['fields'] = $this->definition->getFieldsForIterate($params['fields'] ?? null);
        $iterators = $this->pool->iterate($table, $params);
        return $this->joinIterators($iterators, $params);
    }

    /**
     * @param array $params (optional)
     *
     * @return array
     */
    public function iterateAsArray(array $params = []): array
    {
        return \Amp\Promise\wait(\Amp\call(function ($params) {
            $iterator = $this->iterate($params);
            $elems = yield \Amp\Iterator\toArray($iterator);
            $ret = [];
            foreach ($elems as [$k, $v]) {
                $ret[$k] = $v;
            }
            return $ret;
        }, $params));
    }

    /**
     * @param array $params (optional)
     *
     * @return array
     */
    public function iterateAsGenerator(array $params = []): Generator
    {
        $producer = $this->iterate($params);
        $iterator = function ($producer) {
            if (yield $producer->advance()) {
                return $producer->getCurrent();
            }
        };
        while ($yield = \Amp\Promise\wait(\Amp\call($iterator, $producer))) {
            yield $yield[0] => $yield[1];
        }
    }

    /**
     * @param array $where  (optional)
     * @param mixed $fields (optional)
     *
     * @return array
     */
    public function filter(array $where = [], $fields = null): array
    {
        return $this->iterateAsArray(compact('where', 'fields'));
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return DatabasePool
     */
    public function getShards(?int $id, array $values = []): DatabasePool
    {
        $shards = $this->definition->getShards($id, $values);
        return $this->pool->dbs($shards);
    }

    /**
     * @param ?int  $id
     * @param array $values (optional)
     *
     * @return DatabaseInterface
     */
    public function getShard(?int $id, array $values = []): DatabaseInterface
    {
        $shard = $this->definition->getShard($id, $values);
        return $this->pool->db($shard);
    }

    /**
     * @param array $iterators
     * @param array $params
     *
     * @return Producer
     */
    private function joinIterators(array $iterators, array $params): Producer
    {
        $emit = function ($emit) use ($iterators, $params) {
            $values = [];
            $ids = [];
            $already = [];
            $asc = $params['asc'] ?? true;
            $rand = $params['rand'] ?? false;
            do {
                $keys = array_keys($iterators);
                foreach ($keys as $key) {
                    if (isset($values[$key])) {
                        continue;
                    }
                    do {
                        if (!yield $iterators[$key]->advance()) {
                            unset($iterators[$key]);
                            continue 2;
                        }
                        $value = $iterators[$key]->getCurrent();
                    } while (isset($already[$value[0]]));
                    $values[$key] = $value;
                    $already[$value[0]] = true;
                    $ids[$value[0]] = $key;
                }
                if ($rand) {
                    @ $id = (int) array_rand($ids);
                } else {
                    $keys = array_keys($ids);
                    @ $id = (int) ($asc ? min($keys) : max($keys));
                }
                while (isset($ids[$id])) {
                    $k = $ids[$id];
                    yield $emit($values[$k]);
                    unset($values[$k]);
                    unset($ids[$id]);
                    $id++;
                }
            } while ($iterators || $values);
        };
        return new Producer($emit);
    }
}

    /**
     * @param int $id
     *
     * @return DatabaseInterface
     */
    // public function getDbFromId(int $id): DatabaseInterface
    // {
    //     $shards = $this->definition['shards'];
    //     $n = $id % $shards;
    //     return $this->pool[$n];
    // }

    /**
     * @param array $values
     *
     * @return DatabaseInterface
     */
    // public function getDbFromValues(array $values): DatabaseInterface
    // {
    //     $shards = $this->definition['shards'];
    //     $value = $values[$this->definition['shard_value']];
    //     $n = crc32($value) % $shards;
    //     return $this->pool[$n];
    // }

    // $storage->user()->get(1)
    // $storage->user()->iterator()
    // $storage->user()->all(1)
    // $storage->user()->one(1)
    // $storage->user()->val(1)
    // $storage->user()->col(1)
    // $storage->user()->col(1)
    // $storage->user([1])
    // $storage->user([1, 2])