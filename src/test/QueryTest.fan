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
    echo("==============================================================")

    doTest(
      Filter("ahu"),
      Query(
        "select rec.* from rec
         where
           (rec.paths @> @x0::text[])",
        Str:Obj["x0": "{\"ahu\"}"]))

    doTest(
      Filter("facets->min"),
      Query(
        "select rec.* from rec
         where
           (rec.paths @> @x0::text[])",
        Str:Obj["x0": "{\"facets.min\"}"]))

    doTest(
      Filter("chilledWaterRef->chilled"),
      Query(
        "select rec.* from rec
           inner join pathref p1 on p1.rec_id = rec.id
           inner join rec     r1 on r1.id     = p1.ref_
         where
           ((p1.path_ = @x0) and (r1.paths @> @x1::text[]))",
        Str:Obj["x0":"chilledWaterRef", "x1":"{\"chilled\"}"]))

    //filter := Filter("chilledWaterRef->chilled")
    //query := Query(filter)
    //echo(testData.filter(filter).keys)
    //echo(query)
    //echo(db.select(query))

    echo("==============================================================")
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

  private TestData testData := TestData()
  private Db db := Db()
}
