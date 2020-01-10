<?php

/**
 * @param string $dotenv
 */
function loadDotEnv(string $dotenv)
{
    (new \Dotenv\Dotenv(dirname($dotenv), basename($dotenv)))->load();
}
