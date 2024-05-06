//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

class QueryTest : Test
{
  Void testQuery()
  {
    verifyQuery(
      Query(Filter("point")),
      "select r.hayson from rec as r where (r.hayson ? 'point')",
      Obj[,])

    verifyQuery(
      Query(Filter("compName == \"Services\"")),
      "select r.hayson from rec as r where (r.hayson @> '{\"compName\":?}'::jsonb)",
      Obj["Services"])
  }

  internal Void verifyQuery(Query q, Str sql, Obj[] params)
  {
    echo(q)
    verifyEq(q.sql, sql)
    verifyEq(q.params, params)
  }
}
