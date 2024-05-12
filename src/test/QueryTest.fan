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
    f :=  Filter("ahu")
    expected := testData.filter(f)
    echo(expected.keys)

    q := Query.fromFilter(f)
    echo(q)
    verifyEq(q, Query(
      "select * from rec
       where
         (rec.paths @> @p0::text[])",
      Str:Obj["p0": "{\"ahu\"}"]))

    found := db.select(q)
    echo(found)
    verifyQuery(found, expected)
  }

  private Void verifyQuery(DbRec[] found, Ref:Dict expected)
  {
    verifyEq(found.size, expected.size)
    found.each |fndRec|
    {
      expDict := expected.get(fndRec.id)
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
