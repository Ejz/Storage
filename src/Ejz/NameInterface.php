<?php

namespace Ejz;

interface NameInterface
{
    /**
     * @return string
     */
    public function getName(): string;

    public function setName(string $name);

    public function __toString(): string;
}