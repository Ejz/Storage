<?php

namespace Ejz;

class Bitmap
{
    /** @var RedisClient */
    protected $client;

    /**
     * @param RedisClient $client
     */
    public function __construct(RedisClient $client)
    {
        $this->client = $client;
    }

    /**
     * @param string $cmd
     * @param array  ...$args
     *
     * @return mixed
     */
    public function execute(string $cmd, ...$args)
    {
        $cmd = preg_split('~\s+~', trim($cmd));
        $cmd = array_map(function ($elem) use (&$args) {
            return $elem === '?' ? array_shift($args) : $elem;
        }, $cmd);
        $c = array_shift($cmd);
        return $this->client->$c(...$cmd);
    }
}
