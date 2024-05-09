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
      Query(Filter("ahu")),
      """select * from rec
         where
           (rec.paths @> ?::jsonb);""",
      Str["'{\"ahu\"}'"])

//    verifyQuery(
//      Query(Filter("compName == \"Services\"")),
//      "select r.hayson from rec as r where (r.hayson @> '{\"compName\":?}'::jsonb)",
//      Obj["Services"])
//
//    // it comes out backwards for some reason
//    verifyQuery(
//      Query(Filter("point and links")),
//      "select r.hayson from rec as r where ((r.hayson ? 'links') and (r.hayson ? 'point'))",
//      Obj[,])
  }

  internal Void verifyQuery(Query q, Str sql, Str[] params)
  {
    echo(q)
    verifyEq(q.sql, sql)
    verifyEq(q.params, params)
  }
}
