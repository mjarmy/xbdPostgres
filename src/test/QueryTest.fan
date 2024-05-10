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
    f :=  Filter("ahu")
    expected := testData.filter(f).keys.sort
    echo(expected)

    q :=  Query.fromFilter(f)
    verifyEq(q, Query(
      """select * from rec
         where
           (rec.paths @> ?::jsonb);""",
      Str["'{\"ahu\"}'"]))
  }

  TestData testData := TestData()
}
