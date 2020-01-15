<?php

namespace Ejz;

use Amp\Promise;
use Amp\Iterator;

interface DatabaseInterface
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
    public function val(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function col(string $sql, ...$args): Promise;

    /**
     * @return Promise
     */
    public function tables(): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function fields(string $table): Promise;

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
     *
     * @return Promise
     */
    public function truncate(string $table): Promise;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function drop(string $table): Promise;

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

    /**
     * @param Repository $repository
     *
     * @return Promise
     */
    // public function create(Repository $repository): Promise;

    /**
     * @param Repository $repository
     * @param array      $values
     *
     * @return Promise
     */
    // public function insert(Repository $repository, array $values): Promise;

    /**
     * @param Repository $repository
     * @param int        $id
     * @param array      $values
     *
     * @return Promise
     */
    // public function update(Repository $repository, int $id, array $values): Promise;

    /**
     * @param Repository $repository
     * @param int        $id
     * @param array      $values
     *
     * @return Promise
     */
    // public function delete(Repository $repository, int $id): Promise;
}