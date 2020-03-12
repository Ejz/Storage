<?php

namespace Tests;

use Ejz\WhereCondition;

class TestCaseWhereCondition extends AbstractTestCase
{
    /**
     * @test
     */
    public function test_case_where_condition()
    {
        $db = $this->databasePool->random();
        $db->execSync('CREATE TABLE t (tt TEXT, i INT)');
        $db->execSync('INSERT INTO t (tt, i) VALUES (?, ?), (?, ?)', 'foo', 1, 'bar', 2);
        $cases = [
            [1, ['tt' => 'foo']],
            [2, ['tt' => ['foo', 'bar']]],
            [1, ['tt' => ['foo'], 'i' => [1, 2]]],
            [0, ['tt' => 'bar', 'i' => 1]],
            [2, [['tt', '', '!=']]],
            [1, [['tt', 'fooo', null, 'CONCAT($field, \'o\') $operation $value']]],
            [2, [['tt', ['fooo', 'baro'], null, 'CONCAT($field, \'o\') $operation $value']]],
            [2, [['tt', ['fooo', 'baro'], '=', 'CONCAT($field, \'o\') $operation $value']]],
        ];
        foreach ($cases as [$count, $conditions]) {
            $where = new WhereCondition($conditions);
            $this->assertEquals($count, $db->countSync('t', $where), print_r($conditions, true));
        }
    }
}
