<?php

if (defined('ROOT')) {
    exit(1);
}

define('ROOT', __DIR__);
define('START_TIME', microtime(true));

ini_set('display_errors', 'On');
error_reporting(E_ALL);
date_default_timezone_set('UTC');
mb_internal_encoding('utf-8');
ini_set('default_socket_timeout', 1000);
ini_set('pcre.backtrack_limit', '10000000');

define('DOTENV_FILE', ROOT . '/.env');

require ROOT . '/vendor/autoload.php';
require ROOT . '/container.php';
require ROOT . '/helpers.php';
