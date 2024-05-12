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

  Void testDottedPaths()
  {
    verifyEq(
      QueryBuilder.dottedPaths(Filter("ahu").argA),
      ["ahu"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("facets->min").argA),
      ["facets.min"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("chilledWaterRef->chilled").argA),
      ["chilledWaterRef", "chilled"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("fooOf->barRef").argA),
      ["fooOf", "barRef"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("links->in4->fromRef->meta->inA->flags->linkTarget").argA),
      ["links.in4.fromRef", "meta.inA.flags.linkTarget"])

    verifyEq(
      QueryBuilder.dottedPaths(Filter("equipRef->siteRef->area").argA),
      ["equipRef", "siteRef", "area"])
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
