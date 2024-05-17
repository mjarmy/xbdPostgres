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

  Void testSelectByIds()
  {
    verifyDictsEq(
      haven.selectByIds(Ref[,]),
      Dict[,])

    verifyDictsEq(
      haven.selectByIds(Ref[
        ref("bogus"),
        ref("z0")
      ]),
      Dict[
        testData.recs[ref("z0")]
      ])

    verifyDictsEq(
      haven.selectByIds(Ref[
        ref("z0"),
        ref("z1"),
        ref("z2"),
        ref("z3")
      ]),
      Dict[
        testData.recs[ref("z0")],
        testData.recs[ref("z1")],
        testData.recs[ref("z2")],
        testData.recs[ref("z3")]
      ])
  }

  private Void verifyDictsEq(Dict[] a, Dict[] b)
  {
    verifyEq(a.size, a.size)

    a.sort |Dict x, Dict y->Int| { return x.id.id <=> y.id.id }
    b.sort |Dict x, Dict y->Int| { return x.id.id <=> y.id.id }

    a.each |dict, i|
    {
      verifyTrue(Etc.dictEq(a[i], b[i]))
    }
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

  private static Ref ref(Str str) { Ref.fromStr(str) }

  private TestData testData := TestData()
  private Haven haven := Haven()
}
