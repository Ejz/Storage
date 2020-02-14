<?php

namespace Ejz;

use Amp\Promise;

interface BitmapInterface
{
    /**
     * @param string $index
     *
     * @return Promise
     */
    public function drop(string $index): Promise;

    /**
     * @return Promise
     */
    public function indexes(): Promise;

    /**
     * @param string $index
     *
     * @return Promise
     */
    public function indexExists(string $index): Promise;

    /**
     * @param string $index
     * @param array  $fields (optional)
     *
     * @return Promise
     */
    public function create(string $index, array $fields = []): Promise;

    /**
     * @param string $index
     * @param int    $id
     * @param array  $fields (optional)
     *
     * @return Promise
     */
    public function add(string $index, int $id, array $fields = []): Promise;

    /**
     * @param string $index
     * @param array  $params (optional)
     *
     * @return Iterator
     */
    public function search(string $index, array $params = []): Iterator;
}
