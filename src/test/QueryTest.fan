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
    query :=  Query(Filter("ahu"))
    verifyQuery(
      query,
      """select * from rec
         where
           (rec.paths @> ?::jsonb);""",
      Str["'{\"ahu\"}'"])

//    query =  Query(Filter("facets->min"))
//    echo(query)

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

  internal Void verifyQuery(Query query, Str sql, Str[] params)
  {
    verifyEq(query.sql, sql)
    verifyEq(query.params, params)
  }
}
