<?php

namespace Ejz;

use Generator;

trait GeneratorTrait
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
        while ($yield = \Amp\Promise\wait(\Amp\call($iterator, $this))) {
            yield $yield[0] => $yield[1];
        }
    }
}
