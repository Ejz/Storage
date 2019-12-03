<?php

defined('ROOT') && exit(1);
define('ROOT', __DIR__);
ini_set('display_errors', 'On');
error_reporting(E_ALL);
date_default_timezone_set('UTC');
mb_internal_encoding('utf-8');

if (!is_file($_ = ROOT . '/vendor/autoload.php')) {
    echo 'ERROR: "vendor/autoload.php"', "\n";
    exit(1);
}

require $_;

if (is_file(ROOT . '/.env')) {
    (new \Dotenv\Dotenv(ROOT, '.env'))->load();
}
