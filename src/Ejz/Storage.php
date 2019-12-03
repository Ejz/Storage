<?php

namespace Ejz;

use RuntimeException;

class Storage
{
    /** @var DatabasePool */
    private $pool;

    /** @var array */
    private $tables;

    /**
     * @param DatabasePool $pool
     * @param array        $tables
     */
    public function __construct(
        DatabasePool $pool,
        array $tables
    ) {
        $this->pool = $pool;
        $this->tables = $tables;
    }

    /**
     * @return DatabasePool
     */
    public function getPool(): DatabasePool
    {
        return $this->pool;
    }

    /**
     * @param string $table
     * @param array  $args
     *
     * @return AbstractStorageTable
     */
    public function __call(string $table, array $args): AbstractStorageTable
    {
        $definition = $this->tables[$table] ?? null; 
        if (!$definition) {
            throw new RuntimeException();
        }
        $definition['names'] = $this->pool->names();
        $definition = new TableDefinition($table, $definition);
        return new class (
            $this->pool,
            $definition,
            ...$args
        ) extends AbstractStorageTable {
        };
    }
}