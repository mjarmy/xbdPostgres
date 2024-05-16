//
// Copyright (c) 2024, XetoBase
// All Rights Reserved
//
// History:
//   6 May 2024  Mike Jarmy  Creation
//

using haystack

**
** RecTest
**
class RecTest : Test
{
  Void testRec()
  {
    paths := Str["id"]
    refs := Str:Str["id":"z0"]
    verifyEq(
      Rec.fromDict(testData.recs[id("z0")]),
      Rec(
        "z0",
        paths,
        refs))
  }

  private static Ref id(Str str) { Ref.fromStr(str) }

  private TestData testData := TestData()
}

