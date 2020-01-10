<?php

namespace Ejz;

use Generator;
use Amp\Promise;

interface DatabaseInterface
{
    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function execAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return mixed
     */
    public function exec(string $sql, ...$args);

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function allAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function all(string $sql, ...$args): array;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function oneAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function one(string $sql, ...$args): array;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function valAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return mixed
     */
    public function val(string $sql, ...$args);

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function colAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function col(string $sql, ...$args): array;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function mapAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function map(string $sql, ...$args): array;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return Promise
     */
    public function dictAsync(string $sql, ...$args): Promise;

    /**
     * @param string $sql
     * @param array  ...$args
     *
     * @return array
     */
    public function dict(string $sql, ...$args): array;

    /**
     * @return Promise
     */
    public function tablesAsync(): Promise;

    /**
     * @return array
     */
    public function tables(): array;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function fieldsAsync(string $table): Promise;

    /**
     * @param string $table
     *
     * @return array
     */
    public function fields(string $table): array;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function pksAsync(string $table): Promise;

    /**
     * @param string $table
     *
     * @return ?array
     */
    public function pks(string $table): ?array;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function minAsync(string $table): Promise;

    /**
     * @param string $table
     *
     * @return mixed
     */
    public function min(string $table);

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function maxAsync(string $table): Promise;

    /**
     * @param string $table
     *
     * @return mixed
     */
    public function max(string $table);

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function truncateAsync(string $table): Promise;

    /**
     * @param string $table
     *
     * @return bool
     */
    public function truncate(string $table): bool;

    /**
     * @param string $table
     *
     * @return Promise
     */
    public function dropAsync(string $table): Promise;

    /**
     * @param string $table
     *
     * @return bool
     */
    public function drop(string $table): bool;

    /**
     * @param string $table
     * @param array  $params (optional)
     *
     * @return Generator
     */
    public function iterate(string $table, array $params = []): Generator;
}