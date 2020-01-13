<?php

namespace Ejz;

use Generator;
use Amp\Promise;

interface DatabaseExtendedInterface
{
    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate(string $table, array $params = []): Generator;

    /**
     * @param string $table
     * @param array  $ids
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function get(string $table, array $ids, array $params = []): Generator;

    /**
     * @param Repository $repository
     *
     * @return Promise
     */
    public function createAsync(Repository $definition): Promise;
}
    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    // public function execAsync(string $sql, ...$args): Promise;



    // *
    //  * @param TableDefinition $definition
    //  * @param array           $values
    //  *
    //  * @return Promise
     
    // public function insertAsync(TableDefinition $definition, array $values): Promise;