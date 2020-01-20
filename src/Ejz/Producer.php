<?php

namespace Ejz;

use Generator;
use Amp\Iterator;
use Amp\Promise;

class Producer implements Iterator
{
    /** @var \Amp\Producer */
    private $producer;

    /** @var int */
    private $size;

    /**
     * @param callable $producer
     */
    public function __construct(callable $producer)
    {
        $this->producer = new \Amp\Producer($producer);
    }

    /**
     * @return Promise
     */
    public function advance(): Promise
    {
        return $this->producer->advance();
    }

    /**
     * @return mixed
     */
    public function getCurrent()
    {
        return $this->producer->getCurrent();
    }

    /**
     * @return int
     */
    public function getSize(): int
    {
        return $this->size;
    }

    /**
     * @param int $size
     */
    public function setSize(int $size)
    {
        $this->size = $size;
    }

    /**
     * @return Generator
     */
    public function generator(): Generator
    {
        $iterator = function ($producer) {
            if (yield $producer->advance()) {
                return $producer->getCurrent();
            }
        };
        while (($yield = Promise\wait(\Amp\call($iterator, $this->producer))) !== null) {
            yield $yield[0] => $yield[1];
        }
    }

    /**
     * @param array $iterators
     * @param array $params
     *
     * @return Iterator
     */
    public static function merge(array $iterators, array $params): Iterator
    {
        $emit = function ($emit) use ($iterators, $params) {
            $params += [
                'asc' => true,
                'rand' => false,
            ];
            [
                'asc' => $asc,
                'rand' => $rand,
            ] = $params;
            $inc = $asc ? 1 : -1;
            $values = [];
            $ids = [];
            $already = [];
            do {
                $results = yield array_map(function ($iterator) {
                    return $iterator->advance();
                }, array_diff_key($iterators, $values));
                foreach ($results as $key => $result) {
                    if ($result) {
                        $value = $iterators[$key]->getCurrent();
                        if (!isset($already[$value[0]])) {
                            $already[$value[0]] = true;
                            $values[$key] = $value;
                            $ids[$value[0]] = $key;
                        }
                    } else {
                        unset($iterators[$key]);
                    }
                }
                if (!$values) {
                    if (!$iterators) {
                        break;
                    }
                    continue;
                }
                if ($rand) {
                    foreach ($ids as $id => $k) {
                        yield $emit($values[$k]);
                        unset($values[$k]);
                    }
                    $ids = [];
                    continue;
                }
                $_ids = array_keys($ids);
                $id = $asc ? min($_ids) : max($_ids);
                do {
                    $k = $ids[$id];
                    yield $emit($values[$k]);
                    unset($values[$k]);
                    unset($ids[$id]);
                    $id += $inc;
                } while (isset($ids[$id]));
            } while (true);
        };
        return new self($emit);
    }
}

