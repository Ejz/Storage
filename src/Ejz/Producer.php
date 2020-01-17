<?php

namespace Ejz;

use Generator;
use Amp\Iterator;
use Amp\Promise;

class Producer implements Iterator
{
    use \Amp\CallableMaker;
    use \Amp\Internal\Producer;

    /**
     * @param callable $producer
     */
    public function __construct(callable $producer)
    {
        $result = $producer($this->callableFromInstanceMethod('emit'));
        $coroutine = new \Amp\Coroutine($result);
        $coroutine->onResolve(function ($exception) {
            if ($this->complete) {
                return;
            }
            if ($exception) {
                $this->fail($exception);
                return;
            }
            $this->complete();
        });
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
        while (($yield = Promise\wait(\Amp\call($iterator, $this))) !== null) {
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
                if (!$values && !$iterators) {
                    break;
                }
                if ($rand) {
                    $id = array_rand($ids);
                } else {
                    $_ids = array_keys($ids);
                    $id = $asc ? min($_ids) : max($_ids);
                }
                do {
                    $k = $ids[$id];
                    yield $emit($values[$k]);
                    unset($values[$k]);
                    unset($ids[$id]);
                    $id++;
                } while (isset($ids[$id]));
            } while (true);
        };
        return new self($emit);
    }
}

