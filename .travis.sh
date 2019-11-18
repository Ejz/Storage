#!/usr/bin/env bash

this=`readlink -fe "$0"`
this_dir=`dirname "$this"`
cd "$this_dir"

./deploy.sh || exit 1

if ! which composer; then
    curl -sS 'https://getcomposer.org/installer' | php
    php composer.phar install
    rm -f composer.phar
else
    composer install
fi

test -n "$GH_TOKEN" && php composer.phar config -g github-oauth.github.com "$GH_TOKEN"
file vendor/bin/composer
file vendor/bin/phpunit
