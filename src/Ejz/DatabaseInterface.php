<?php

namespace Ejz;

use Amp\Promise;

interface DatabaseInterface extends NameInterface
{
    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function exec(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function all(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function one(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function col(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function val(string $sql, ...$args): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function drop(string $table): Promise;

    /**
     * @return Promise
     */
    public function tables(): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function tableExists(string $table): Promise;

    /**
     * @param string          $table
     * @param ?WhereCondition $where (optional)
     *
     * @return Promise
     */
    public function count(string $table, ?WhereCondition $where): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function truncate(string $table): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function fields(string $table): Promise;

    /**
     * @param string $table
     * @param string $field
     *
     * @return Promise
     */
    public function fieldExists(string $table, string $field): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function indexes(string $table): Promise;

    /**
     * @param string $table
     * @param string $index
     *
     * @return Promise
     */
    public function indexExists(string $table, string $index): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pk(string $table): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function min(string $table): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function max(string $table): Promise;

    /**
     * @param string $table
     * @param string $primaryKey
     * @param int    $primaryKeyStart     (optional)
     * @param int    $primaryKeyIncrement (optional)
     * @param array  $fields              (optional)
     * @param array  $indexes             (optional)
     * @param array  $foreignKeys         (optional)
     *
     * @return Promise
     */
    public function create(
        string $table,
        string $primaryKey,
        int $primaryKeyStart = 1,
        int $primaryKeyIncrement = 1,
        array $fields = [],
        array $indexes = [],
        array $foreignKeys = []
    ): Promise;

    /**
     * @param string $table
     * @param string $primaryKey
     * @param ?int   $int        (optional)
     * @param array  $fields     (optional)
     *
     * @return Promise
     */
    public function insert(
        string $table,
        string $primaryKey,
        ?int $id = null,
        array $fields = []
    ): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function getNextId(string $table): Promise;

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     * @param array  $fields
     *
     * @return Promise
     */
    public function update(
        string $table,
        string $primaryKey,
        array $ids,
        array $fields
    ): Promise;

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     *
     * @return Promise
     */
    public function delete(string $table, string $primaryKey, array $ids): Promise;

    /**
     * @param string $table
     * @param string $primaryKey
     * @param array  $ids
     *
     * @return Promise
     */
    public function reid(string $table, string $primaryKey, array $ids): Promise;

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function iterate(string $table, array $params = []): Iterator;

    /**
     * @param string $table
     * @param array  $ids
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function get(string $table, array $ids, array $params = []): Iterator;
}