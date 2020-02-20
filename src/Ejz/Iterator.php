<?php

namespace Ejz;

use Amp\Promise;

class Iterator implements \Amp\Iterator, \Iterator, ContextInterface
{
    use SyncTrait;
    use ContextTrait;

    /** @var \Amp\Iterator */
    private $iterator;

    /** @var ?bool */
    private $next;

    /**
     * @param mixed $iterator (optional)
     */
    public function __construct($iterator = null)
    {
        if ($iterator !== null) {
            $this->setIterator($iterator);
        }
    }

    /**
     * @return \Amp\Iterator
     */
    public function getIterator(): \Amp\Iterator
    {
        return $this->iterator;
    }

    /**
     * @param mixed $iterator
     */
    public function setIterator($iterator)
    {
        if (!is_callable($iterator)) {
            $_ = $iterator;
            $iterator = function ($emit) use ($_) {
                foreach ($_ as $value) {
                    yield $emit($value);
                }
            };
        }
        $this->iterator = new \Amp\Producer($iterator);
        $this->next = null;
    }

    /**
     * @return Promise
     */
    public function advance(): Promise
    {
        return $this->iterator->advance();
    }

    /**
     * @return mixed
     */
    public function getCurrent()
    {
        return $this->iterator->getCurrent();
    }

    /**
     * @return mixed
     */
    public function current()
    {
        if ($this->next === null) {
            $this->next();
        }
        return $this->next === true ? $this->iterator->getCurrent()[1] : null;
    }

    /**
     * @return int
     */
    public function key(): ?int
    {
        if ($this->next === null) {
            $this->next();
        }
        return $this->next === true ? $this->iterator->getCurrent()[0] : null;
    }

    /**
     */
    public function next()
    {
        $this->next = $this->advanceSync();
    }

    /**
     */
    public function valid(): bool
    {
        if ($this->next === null) {
            $this->next();
        }
        return $this->next === true;
    }

    /**
     */
    public function rewind()
    {
    }

    /**
     * @param array    $iterators
     * @param callable $sort
     *
     * @return self
     */
    public static function merge(array $iterators, callable $sort): self
    {
        if (count($iterators) === 1) {
            return current($iterators);
        }
        $iterator = new self();
        $emit = function ($emit) use ($iterators, $sort, $iterator) {
            $values = [];
            $ids = [];
            while (true) {
                do {
                    $diff = array_diff_key($iterators, $values);
                    $results = yield array_map(function ($iterator) {
                        return $iterator->advance();
                    }, $diff);
                    foreach ($results as $key => $value) {
                        if ($value === false) {
                            unset($iterators[$key]);
                            unset($diff[$key]);
                            continue;
                        }
                        $value = $iterators[$key]->getCurrent();
                        if (!isset($ids[$value[0]])) {
                            $ids[$value[0]] = true;
                            $values[$key] = $value;
                            unset($diff[$key]);
                        }
                    }
                } while ($diff);
                if (!$values) {
                    break;
                }
                uasort($values, $sort);
                $key = key($values);
                yield $emit($values[$key]);
                unset($values[$key]);
            }
        };
        $iterator->setIterator($emit);
        return $iterator;
    }

    /**
     * @param self     $iterator
     * @param callable $map
     *
     * @return self
     */
    public static function map(self $iterator, callable $map): self
    {
        $emit = function ($emit) use ($iterator, $map) {
            while (yield $iterator->advance()) {
                yield $emit($map($iterator->getCurrent()));
            }
        };
        return new self($emit);
    }

    /**
     * @param self $iterator
     * @param int  $size
     *
     * @return self
     */
    public static function chunk(self $iterator, int $size): self
    {
        $emit = function ($emit) use ($iterator, $size) {
            $collect = [];
            while (yield $iterator->advance()) {
                $collect[] = $iterator->getCurrent();
                if (count($collect) === $size) {
                    yield $emit($collect);
                    $collect = [];
                }
            }
            if ($collect) {
                yield $emit($collect);
            }
        };
        return new self($emit);
    }

    /**
     * @param array $iterators
     *
     * @return self
     */
    public static function pair(array $iterators): self
    {
        $emit = function ($emit) use ($iterators) {
            $c = count($iterators);
            while ($c > 0) {
                $results = yield array_map(function ($iterator) {
                    return $iterator->advance();
                }, $iterators);
                if (count(array_filter($results)) !== $c) {
                    break;
                }
                $results = array_map(function ($iterator) {
                    return $iterator->getCurrent();
                }, $iterators);
                yield $emit(array_values($results));
            }
        };
        return new self($emit);
    }

    /**
     * @param self $iterator
     *
     * @return self
     */
    public static function wrap(self $iterator): self
    {
        return self::map($iterator, function ($value) {
            $id = array_shift($value);
            return [$id, $value];
        });
    }

    /**
     * @param self     $iterator
     * @param callable $filter
     *
     * @return self
     */
    public static function filter(self $iterator, callable $filter): self
    {
        $emit = function ($emit) use ($iterator, $filter) {
            while (yield $iterator->advance()) {
                $value = $iterator->getCurrent();
                if ($filter($value)) {
                    yield $emit($value);
                }
            }
        };
        return new self($emit);
    }

    /**
     * @param self $iterator
     * @param int  $offset
     *
     * @return self
     */
    public static function offset(self $iterator, int $offset): self
    {
        $position = 0;
        return self::filter($iterator, function ($value) use (&$position, $offset) {
            return $position++ >= $offset;
        });
    }

    /**
     * @param self $iterator
     * @param int  $limit
     *
     * @return self
     */
    public static function limit(self $iterator, int $limit): self
    {
        $emit = function ($emit) use ($iterator, $limit) {
            while (($limit-- > 0) && (yield $iterator->advance())) {
                yield $emit($iterator->getCurrent());
            }
        };
        return new self($emit);
    }

    /**
     * @param self $iterator
     * @param int  $offset
     * @param int  $limit
     *
     * @return self
     */
    public static function offsetLimit(self $iterator, int $offset, int $limit): self
    {
        $iterator = self::offset($iterator, $offset);
        $iterator = self::limit($iterator, $limit);
        return $iterator;
    }
}
