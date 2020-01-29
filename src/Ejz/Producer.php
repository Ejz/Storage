<?php

namespace Ejz;

use Generator;
use Amp\Iterator;
use Amp\Promise;

class Producer implements Iterator
{
    /** @var \Amp\Producer */
    private $iterator;

    /** @var int */
    private $size;

    /** @var ?string */
    private $cursor;

    /** @var ?array */
    private $iterators;

    /**
     * @param mixed $iterator (optional)
     */
    public function __construct($iterator = null)
    {
        $this->setIterator($iterator);
    }

    /**
     * @return Promise
     */
    public function advance(): Promise
    {
        return $this->iterator->advance();
    }

    /**
     * @return bool
     */
    public function advanceSync(): bool
    {
        return Promise\wait($this->iterator->advance());
    }

    /**
     * @return mixed
     */
    public function getCurrent()
    {
        return $this->iterator->getCurrent();
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
     * @return string
     */
    public function getCursor(): ?string
    {
        return $this->cursor;
    }

    /**
     * @param string $cursor
     */
    public function setCursor(?string $cursor)
    {
        $this->cursor = $cursor;
    }

    /**
     * @param mixed $iterator
     */
    public function setIterator($iterator)
    {
        $this->iterator = $iterator;
        if (is_callable($this->iterator)) {
            $this->iterator = new \Amp\Producer($this->iterator);
        }
    }

    /**
     * @return mixed
     */
    public function getIterator()
    {
        return $this->iterator;
    }

    public function setBitmap($bitmap)
    {
        $this->bitmap = $bitmap;
    }

    /**
     * @return array
     */
    public function getSearchIteratorState(): array
    {
        if ($this->iterators === null) {
            $searchState = $this->bitmap->getSearchIteratorState($this->cursor);
            return $searchState + ['size' => $this->size, 'cursor' => $this->cursor];
        }
        return array_map(function ($iterator) {
            return $iterator->getSearchIteratorState();
        }, $this->iterators);
    }

    // /**
    //  * @param array $searchState
    //  */
    // public function setSearchState(array $searchState)
    // {
    //     $this->searchState = $searchState;
    // }

    // /**
    //  */
    // public function moveSearchStatePointer()
    // {
    //     var_dump($this->searchState);
    //     // var_dump(debug_print_backtrace());
    //     $this->searchState['pointer']++;
    // }

    /**
     * @return Generator
     */
    public function generator(): Generator
    {
        $iterator = function ($iterator) {
            if (yield $iterator->advance()) {
                return $iterator->getCurrent();
            }
        };
        while (($yield = Promise\wait(\Amp\call($iterator, $this->iterator))) !== null) {
            yield $yield[0] => $yield[1];
        }
    }

    /**
     * @param Iterator $iterator
     * @param array    $ids
     *
     * @return Iterator
     */
    public static function getIteratorWithIdsOrder(Iterator $iterator, array $ids): Iterator
    {
        $emitter = new \Amp\Emitter();
        $return = $emitter->iterate();
        $collect = [];
        $p1 = \Amp\call(function () use (&$collect, &$iterator, &$emitter) {
            while ((yield $iterator->advance()) && $emitter !== null) {
                [$id, $bean] = $iterator->getCurrent();
                $collect[$id] = $bean;
            }
            $iterator = null;
        });
        $p2 = \Amp\call(function ($ids) use (&$collect, &$iterator, &$emitter) {
            $pointer = 0;
            $count = count($ids);
            while ($iterator !== null && $emitter !== null && $pointer < $count) {
                $id = $ids[$pointer];
                if (isset($collect[$id])) {
                    yield $emitter->emit([$id, $collect[$id]]);
                    $pointer++;
                } else {
                    yield \Amp\delay(20);
                }
            }
            if ($emitter === null) {
                return;
            }
            for (; $pointer < $count; $pointer++) {
                $id = $ids[$pointer];
                if (isset($collect[$id])) {
                    yield $emitter->emit([$id, $collect[$id]]);
                }
            }
        }, $ids);
        Promise\all([$p1, $p2])->onResolve(function ($exception) use (&$emitter) {
            if ($exception) {
                $emitter->fail($exception);
                $emitter = null;
            } else {
                $emitter->complete();
            }
        });
        return new self($return);
    }

    /**
     * @param array    $iterators
     * @param callable $score
     *
     * @return Iterator
     */
    public static function getIteratorWithSortedValues(array $iterators, callable $score): Iterator
    {
        $emit = function ($emit) use ($iterators, $score) {
            $values = [];
            while (true) {
                $results = yield array_map(function ($iterator) {
                    return $iterator->advance();
                }, array_diff_key($iterators, $values));
                foreach ($results as $key => $result) {
                    if ($result) {
                        $value = $iterators[$key]->getCurrent();
                        $values[$key] = $value;
                    } else {
                        unset($iterators[$key]);
                    }
                }
                if (!$values) {
                    break;
                }
                uasort($values, function ($v1, $v2) use ($score) {
                    $s1 = $score($v1[1]);
                    $s2 = $score($v2[1]);
                    if (!$s1 && !$s2) {
                        return $v1[0] > $v2[0];
                    }
                    return $s1 < $s2;
                });
                $key = key($values);
                yield $emit($values[$key]);
                unset($values[$key]);
            }
        };
        $iterator = new self($emit);
        $iterator->setIterators($iterators);
        return $iterator;
    }

    public function setIterators($iterators)
    {
        $this->iterators = $iterators;
    }
}

