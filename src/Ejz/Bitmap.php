<?php

namespace Ejz;

use Amp\Promise;
use Amp\Success;
use Amp\Deferred;

class Bitmap implements NameInterface
{
    use NameTrait;
    use SyncTrait;

    /** @var dsn */
    private $dsn;

    /** @var array */
    private $config;

    /** @var string */
    private const NO_RESULTS_ERROR = 'NO_RESULTS_ERROR';

    /**
     * @param string $name
     * @param string $dsn
     */
    public function __construct(string $name, string $dsn, array $config = [])
    {
        $this->setName($name);
        $this->dsn = $dsn;
        $this->config = $config + [
            'iterator_chunk_size' => 100,
        ];
    }

    /**
     * @return Promise
     */
    public function indexes(): Promise
    {
        return $this->query('LIST');
    }

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function drop(string $index): Promise
    {
        return \Amp\call(function ($index) {
            if (!yield $this->indexExists($index)) {
                return false;
            }
            $this->query('DROP #', $index);
            return true;
        }, $index);
    }

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function truncate(string $index): Promise
    {
        return \Amp\call(function ($index) {
            if (!yield $this->indexExists($index)) {
                return false;
            }
            $this->query('TRUNCATE #', $index);
            return true;
        }, $index);
    }

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function indexExists(string $index): Promise
    {
        return \Amp\call(function ($index) {
            return in_array($index, yield $this->indexes());
        }, $index);
    }

    /**
     * @param string $index
     * @param array  $fields (optional)
     *
     * @return Promise
     */
    public function create(string $index, array $fields = []): Promise
    {
        $query = 'CREATE # FIELDS';
        $args = [$index];
        foreach ($fields as $field) {
            $query .= ' #';
            $args[] = $field->getName();
            $type = $field->getType();
            $query .= ' ' . $type->getName();
            if (method_exists($type, 'getCreateOptions')) {
                [$_query, $_args] = $type->getCreateOptions();
                $query .= $_query ? ' ' . $_query : '';
                $args = array_merge($args, $_args);
            }
        }
        return $this->query($query, ...$args);
        
    }

    /**
     * @param string $index
     * @param int    $id
     * @param array  $fields (optional)
     *
     * @return Promise
     */
    public function add(string $index, int $id, array $fields = []): Promise
    {
        $query = 'ADD # ? VALUES';
        $args = [$index, $id];
        foreach ($fields as $field) {
            $query .= ' # ?';
            $args[] = $field->getName();
            $args[] = $field->exportValue();
        }
        return $this->query($query, ...$args);
    }

    /**
     * @param string $index
     * @param array  $params (optional)
     *
     * @return Iterator|array
     */
    public function search(string $index, array $params = [])
    {
        $count0 = count($params);
        $count1 = count(array_filter(array_keys($params), 'is_string'));
        $is_assoc = $count0 === $count1;
        $array_of_params = $is_assoc ? [$params] : $params;
        $iterators = [];
        $queries = [];
        $promises = [];
        $pointer = 0;
        $max = count($array_of_params);
        $resolver = function ($query, $args, $wait) use (&$queries, &$promises, &$pointer, $max) {
            $deferred = new Deferred();
            $promise = $deferred->promise();
            $queries[$pointer] = [$query, $args];
            $promises[$pointer] = $deferred;
            $pointer++;
            if (count($queries) === $max || !$wait) {
                $ids = array_keys($queries);
                $onResolve = function ($err, $res) use ($ids, &$promises) {
                    foreach (($err ? $ids : []) as $id) {
                        $promise = $promises[$id];
                        unset($promises[$id]);
                        $promise->fail($err);
                    }
                    foreach (($err ? [] : $res) as $id => $result) {
                        $promise = $promises[$id];
                        unset($promises[$id]);
                        if (array_key_exists('error', $result)) {
                            $promise->fail(new BitmapException($result['error']));
                        } else {
                            $promise->resolve($result['result']);
                        }
                    }
                };
                $p = $this->sendQueries($queries);
                $p->onResolve($onResolve);
                $queries = [];
            }
            return $promise;
        };
        foreach ($array_of_params as $params) {
            $iterator = new Iterator();
            $emit = function ($emit) use ($index, $params, $iterator, $resolver) {
                $params += [
                    'query' => '*',
                    'sortby' => null,
                    'asc' => true,
                    'min' => null,
                    'max' => null,
                    'foreignKeys' => [],
                    'forceForeignKeyFormat' => false,
                    'emitTotal' => false,
                    'config' => [],
                ];
                [
                    'query' => $query,
                    'sortby' => $sortby,
                    'asc' => $asc,
                    'min' => $min,
                    'max' => $max,
                    'foreignKeys' => $foreignKeys,
                    'forceForeignKeyFormat' => $forceForeignKeyFormat,
                    'emitTotal' => $emitTotal,
                    'config' => $config,
                ] = $params;
                $config += $this->config;
                [
                    'iterator_chunk_size' => $iterator_chunk_size,
                ] = $config;
                $command = 'SEARCH # ?';
                $query = $min !== null ? "({$query}) & (@id >= {$min})" : $query;
                $query = $max !== null ? "({$query}) & (@id <= {$max})" : $query;
                $args = [$index, $query];
                if ($sortby !== null) {
                    $command .= ' SORTBY # ' . ($asc ? 'ASC' : 'DESC');
                    $args[] = $sortby;
                }
                $foreignKeys = (array) $foreignKeys;
                if ($foreignKeys) {
                    $command .= ' WITHFOREIGNKEYS' . str_repeat(' #', count($foreignKeys));
                    $args = array_merge($args, $foreignKeys);
                }
                $command .= ' WITHCURSOR TIMEOUT 3600';
                $command .= ' LIMIT ' . $iterator_chunk_size;
                $response = yield $resolver($command, $args, true);
                if ($emitTotal) {
                    yield $emit($response['total']);
                }
                $cursor = $response['cursor'];
                if (isset($response['ids']) && $forceForeignKeyFormat) {
                    $response['records'] = array_map(function ($id) {
                        return compact('id');
                    }, $response['ids']);
                    unset($response['ids']);
                }
                foreach (($response['ids'] ?? $response['records']) as $elem) {
                    yield $emit($elem);
                }
                while ($cursor) {
                    $command = 'CURSOR #';
                    $args = [$cursor];
                    $response = yield $resolver($command, $args, false);
                    $cursor = $response['cursor'];
                    if (isset($response['ids']) && $forceForeignKeyFormat) {
                        $response['records'] = array_map(function ($id) {
                            return compact('id');
                        }, $response['ids']);
                        unset($response['ids']);
                    }
                    foreach (($response['ids'] ?? $response['records']) as $elem) {
                        yield $emit($elem);
                    }
                }
            };
            $iterator->setIterator($emit);
            $iterators[] = $iterator;
        }
        return $is_assoc ? $iterators[0] : $iterators;
    }

    /**
     * @param string $query
     * @param array  ...$args
     *
     * @return Promise
     */
    private function query(string $query, ...$args): Promise
    {
        return \Amp\call(function ($query, $args) {
            [$result] = yield $this->sendQueries([[$query, $args]]);
            if (array_key_exists('error', $result)) {
                throw new BitmapException($result['error']);
            }
            return $result['result'];
        }, $query, $args);
    }

    /**
     * @param array $queries
     *
     * @return Promise
     */
    private function sendQueries(array $queries): Promise
    {
        $ch = \curl_init($this->dsn);
        $collect = [];
        foreach ($queries as $id => [$query, $args]) {
            $collect[] = [
                'id' => $id,
                'query' => $this->substitute($query, $args),
            ];
        }
        \curl_setopt($ch, \CURLOPT_POSTFIELDS, \json_encode($collect));
        \curl_setopt($ch, \CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
        \curl_setopt($ch, \CURLOPT_RETURNTRANSFER, true);
        $results = \curl_exec($ch);
        \curl_close($ch);
        $results = \json_decode($results, true);
        if (!$results) {
            throw new BitmapException(self::NO_RESULTS_ERROR);
        }
        $collect = [];
        foreach ($results as $result) {
            $id = $result['id'];
            unset($result['id']);
            $collect[$id] = $result;
        }
        return new Success($collect);
    }

    /**
     * @param string $query
     * @param array  $args
     *
     * @return string
     */
    private function substitute(string $query, array $args): string
    {
        $query = trim($query);
        $query = preg_replace('~\s+~', ' ', $query);
        $args = array_values($args);
        $pos = 0;
        $e = ['\'' => '\\\'', '\\' => '\\\\'];
        $query = preg_replace_callback('~((\\?|#|%)+)~i', function ($match) use (&$pos, $args, $e) {
            $len = strlen($match[0]);
            $single = $match[0][0];
            $return = str_repeat($single, (int) ($len / 2));
            if ($len % 2 === 1) {
                $value = $args[$pos++] ?? null;
                if ($single === '?') {
                    if ($value === null) {
                        $value = 'UNDEFINED';
                    } else {
                        $value = is_int($value) ? $value : '\'' . strtr($value, $e) . '\'';
                    }
                } elseif ($single === '#') {
                    $value = '"' . $value . '"';
                }
                $return .= $value;
            }
            return $return;
        }, $query);
        return trim($query);
    }
}
