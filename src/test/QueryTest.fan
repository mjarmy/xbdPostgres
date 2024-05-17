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
    haven.open(
      "jdbc:postgresql://localhost/postgres",
      "xbd",
      "s3crkEt")
  }

  override Void teardown()
  {
    haven.close()
  }

  Void testSelectById()
  {
    verifyTrue(
      Etc.dictEq(
        haven.selectById(ref("z0")),
        testData.recs[ref("z0")]
      ))

    verifyTrue(haven.selectById(ref("bogus")) == null)
  }

//  Void testDottedPaths()
//  {
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("ahu").argA),
//      ["ahu"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("facets->min").argA),
//      ["facets.min"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("chilledWaterRef->chilled").argA),
//      ["chilledWaterRef", "chilled"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("fooOf->barRef").argA),
//      ["fooOf", "barRef"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("links->in4->fromRef->meta->inA->flags->linkTarget").argA),
//      ["links.in4.fromRef", "meta.inA.flags.linkTarget"])
//
//    verifyEq(
//      QueryBuilder.dottedPaths(Filter("equipRef->siteRef->area").argA),
//      ["equipRef", "siteRef", "area"])
//  }

  private static Ref ref(Str str) { Ref.fromStr(str) }

  private TestData testData := TestData()
  private Haven haven := Haven()
}
