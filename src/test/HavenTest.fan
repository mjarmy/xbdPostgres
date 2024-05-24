//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   22 May 2024  Mike Jarmy  Creation
//

using haystack

**
** HavenTest
**
class HavenTest : Test
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

  Void testRefPaths()
  {
    verifyEq(
      haven.refPaths(Filter("ahu").argA),
      ["ahu"])

    verifyEq(
      haven.refPaths(Filter("facets->min").argA),
      ["facets.min"])

    verifyEq(
      haven.refPaths(Filter("chilledWaterRef->chilled").argA),
      ["chilledWaterRef", "chilled"])

    verifyEq(
      haven.refPaths(Filter("links->in4->fromRef->meta->inA->flags->linkTarget").argA),
      ["links.in4.fromRef", "meta.inA.flags.linkTarget"])

    verifyEq(
      haven.refPaths(Filter("equipRef->siteRef->area").argA),
      ["equipRef", "siteRef", "area"])
  }

  Void testLastTag()
  {
    verifyEq(Haven.lastTag("a"), "a")
    verifyEq(Haven.lastTag("a.b"), "b")
    verifyEq(Haven.lastTag("aa.bb.cc.dd"), "dd")
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  private Haven haven := Haven()
}
