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

    /** @var string */
    private $cursor;

    /** @var mixed */
    private $context;

    /**
     * @param mixed $iterator
     */
    public function __construct($iterator)
    {
        $this->iterator = $iterator;
        if (is_callable($this->iterator)) {
            $this->iterator = new \Amp\Producer($this->iterator);
        }
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
    public function getCursor(): string
    {
        return $this->cursor;
    }

    /**
     * @param string $cursor
     */
    public function setCursor(string $cursor)
    {
        $this->cursor = $cursor;
    }

    /**
     * @return mixed
     */
    public function getContext()
    {
        return $this->context;
    }

    /**
     * @param mixed $context
     */
    public function setContext($context)
    {
        $this->context = $context;
    }

    /**
     * @return array
     */
    public function getSearchIteratorState(): array
    {
        if ($this->context instanceof Bitmap) {
            $state = $this->context->getSearchIteratorState($this->cursor);
            return ['size' => $this->size, 'cursor' => $this->cursor] + $state;
        }
        [
            'repository' => $repository,
            'iterators' => $iterators,
        ] = $this->context;
        $ids = $repository->getSearchIteratorState($this->cursor);
        $collect = [];
        foreach ($iterators as $name => $iterator) {
            $state = $iterator->getSearchIteratorState();
            if (isset($ids[$name])) {
                $_pointer = $state['pointer'] ?? 0;
                $_ids = $state['ids'] ?? [];
                if ($_pointer > 0) {
                    $_ids = array_slice($_ids, $_pointer);
                }
                array_unshift($_ids, $ids[$name]);
                $state['ids'] = $_ids;
                $state['pointer'] = 0;
            }
            $collect[$name] = $state;
        }
        return $collect;
        // if ($this->context instanceof Repository) {
        //     $state = $this->context->getSearchIteratorState($this->cursor);
        //     return $state;
        // }
        // if ($this->iterators === null) {
            
        // }
        // return array_map(function ($iterator) {
        //     return $iterator->getSearchIteratorState();
        // }, $this->iterators);
        // $iterators = $this->iterators ?? [$this->bitmap->getName() => $this->bitmap];
        // $collect = [];
        // foreach ($iterators as $name => $iterator) {
            

        //     $collect += 
        //     $_ = $this->iterators === null ? $this : $iterator;
        //     $collect[$name]['size'] = $_->getSize();
        // }
        // return $collect;
        // if ($this->iterators === null) {
        //     // var_dump($this->bitmap->getName() . '-' . $this->cursor);
        //     $searchState = $this->bitmap->getSearchIteratorState($this->cursor);
        //     $state = $searchState + ;
        //     return [$this->bitmap->getName() => $state];
        // }
        
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

    // /**
    //  * @param Iterator $iterator
    //  * @param array    $ids
    //  *
    //  * @return Iterator
    //  */
    // public static function getIteratorWithIdsOrder(Iterator $iterator, array $ids): Iterator
    // {
    //     $emitter = new \Amp\Emitter();
    //     $return = $emitter->iterate();
    //     $collect = [];
    //     $p1 = \Amp\call(function () use (&$collect, &$iterator, &$emitter) {
    //         while ((yield $iterator->advance()) && $emitter !== null) {
    //             [$id, $bean] = $iterator->getCurrent();
    //             $collect[$id] = $bean;
    //         }
    //         $iterator = null;
    //     });
    //     $p2 = \Amp\call(function ($ids) use (&$collect, &$iterator, &$emitter) {
    //         $pointer = 0;
    //         $count = count($ids);
    //         while ($iterator !== null && $emitter !== null && $pointer < $count) {
    //             $id = $ids[$pointer];
    //             if (isset($collect[$id])) {
    //                 yield $emitter->emit([$id, $collect[$id]]);
    //                 $pointer++;
    //             } else {
    //                 yield \Amp\delay(20);
    //             }
    //         }
    //         if ($emitter === null) {
    //             return;
    //         }
    //         for (; $pointer < $count; $pointer++) {
    //             $id = $ids[$pointer];
    //             if (isset($collect[$id])) {
    //                 yield $emitter->emit([$id, $collect[$id]]);
    //             }
    //         }
    //     }, $ids);
    //     Promise\all([$p1, $p2])->onResolve(function ($exception) use (&$emitter) {
    //         if ($exception) {
    //             $emitter->fail($exception);
    //             $emitter = null;
    //         } else {
    //             $emitter->complete();
    //         }
    //     });
    //     return new self($return);
    // }

    // /**
    //  * @param array    $iterators
    //  * @param callable $score
    //  *
    //  * @return Iterator
    //  */
    // public static function getIteratorWithSortedValues(array $iterators, callable $score): Iterator
    // {
    //     $emit = function ($emit) use ($iterators, $score) {
    //         $values = [];
    //         while (true) {
    //             $results = yield array_map(function ($iterator) {
    //                 return $iterator->advance();
    //             }, array_diff_key($iterators, $values));
    //             foreach ($results as $key => $result) {
    //                 if ($result) {
    //                     $value = $iterators[$key]->getCurrent();
    //                     $values[$key] = $value;
    //                 } else {
    //                     unset($iterators[$key]);
    //                 }
    //             }
    //             if (!$values) {
    //                 break;
    //             }
    //             uasort($values, function ($v1, $v2) use ($score) {
    //                 $s1 = $score($v1[1]);
    //                 $s2 = $score($v2[1]);
    //                 if (!$s1 && !$s2) {
    //                     return $v1[0] > $v2[0];
    //                 }
    //                 return $s1 < $s2;
    //             });
    //             $key = key($values);
    //             yield $emit($values[$key]);
    //             unset($values[$key]);
    //         }
    //     };
    //     $iterator = new self($emit);
    //     $iterator->setIterators($iterators);
    //     return $iterator;
    // }
}

