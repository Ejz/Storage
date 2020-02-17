<?php

namespace Ejz;

use Amp\Promise;

class Iterator implements \Amp\Iterator, \Iterator
{
    use SyncTrait;

    /** @var \Amp\Iterator */
    private $iterator;

    /** @var ?bool */
    private $next;

    /** @var array */
    private $context;

    /**
     * @param mixed $iterator (optional)
     */
    public function __construct($iterator = null)
    {
        if ($iterator !== null) {
            $this->setIterator($iterator);
        }
        $this->context = [];
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
     * @return array
     */
    public function getContext(): array
    {
        return $this->context;
    }

    /**
     * @param mixed   $value
     * @param ?string $key   (optional)
     */
    public function setContext($value, ?string $key = null)
    {
        if ($key === null) {
            $this->context = $value;
        } else {
            $this->context[$key] = $value;
        }
    }

    /**
     * @param array     $iterators
     * @param ?callable $sort      (optional)
     * @param ?callable $saver     (optional)
     *
     * @return self
     */
    public static function merge(
        array $iterators,
        ?callable $sort = null,
        ?callable $saver = null
    ): self
    {
        if (count($iterators) === 1) {
            return current($iterators);
        }
        $iterator = new self();
        $emit = function ($emit) use ($iterators, $sort, $saver, $iterator) {
            $values = [];
            $ids = [];
            $state = [];
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
                            $state[$key] = $value[0];
                            unset($diff[$key]);
                        }
                    }
                } while ($diff);
                if (!$values) {
                    break;
                }
                if ($sort !== null) {
                    uasort($values, $sort);
                    $key = key($values);
                } else {
                    $key = array_rand($values);
                }
                if ($saver !== null) {
                    $saver($iterator, $state);
                }
                yield $emit($values[$key]);
                unset($values[$key], $state[$key]);
                if ($saver !== null) {
                    $saver($iterator, $state);
                }
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
     * @return array
     */
    public function getSearchState(): array
    {
        ['ids' => $ids, 'iterators' => $iterators] = $this->context;
        $contexts = array_map(function ($iterator) {
            return $iterator->getContext();
        }, $iterators);
        foreach ($contexts as $key => &$context) {
            if (isset($ids[$key])) {
                array_unshift($context, $ids[$key]);
            }
        }
        unset($context);
        return $contexts;
    }
}
