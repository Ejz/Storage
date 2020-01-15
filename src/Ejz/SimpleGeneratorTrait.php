<?php

namespace Ejz;

use Generator;

trait SimpleGeneratorTrait
{
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
        while (($yield = \Amp\Promise\wait(\Amp\call($iterator, $this))) !== null) {
            yield $yield;
        }
    }
}
