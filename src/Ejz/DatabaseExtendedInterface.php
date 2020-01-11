<?php

namespace Ejz;

use Generator;

interface DatabaseExtendedInterface
{
    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate(string $table, array $params = []): Generator;
}
    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    // public function execAsync(string $sql, ...$args): Promise;

// /**
    //  * @param TableDefinition $definition
    //  *
    //  * @return Promise
    //  */
    // public function createAsync(TableDefinition $definition): Promise;

    // *
    //  * @param TableDefinition $definition
    //  * @param array           $values
    //  *
    //  * @return Promise
     
    // public function insertAsync(TableDefinition $definition, array $values): Promise;