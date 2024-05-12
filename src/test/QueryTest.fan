//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** QueryTest
**
class QueryTest : Test
{
  override Void setup()
  {
    db.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")
  }

  override Void teardown()
  {
    db.close()
  }

  Void testQuery()
  {
    doTest(
      Filter("ahu"),
      Query(
        "select * from rec
         where
           (rec.paths @> @p0::text[])",
        Str:Obj["p0": "{\"ahu\"}"]))

    doTest(
      Filter("facets->min"),
      Query(
        "select * from rec
         where
           (rec.paths @> @p0::text[])",
        Str:Obj["p0": "{\"facets.min\"}"]))
  }

  Void doTest(
    Filter filter,
    Query expQuery)
  {
    echo("-----------------------------------")
    expDicts := testData.filter(filter)
    echo(expDicts.keys)

    query := Query.fromFilter(filter)
    echo(query)
    verifyEq(query, expQuery)

    found := db.select(query)
    //echo(found)
    verifyFound(found, expDicts)
  }

  private Void verifyFound(DbRec[] found, Ref:Dict expDicts)
  {
    verifyEq(found.size, expDicts.size)
    found.each |fndRec|
    {
      expDict := expDicts.get(fndRec.id)
      expRec := DbRec.fromDict(expDict)
      verifyEq(fndRec, expRec)

      // TODO transform fndRec into a Dict and verify against expDict
    }
  }

  //-----------------------------------------------
  // Fields
  //-----------------------------------------------

  private TestData testData := TestData()
  private Db db := Db()
}
