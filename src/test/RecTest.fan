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
    verifyEq(
      Rec.fromDict(
        Etc.dict1(
          "id", ref("z0")
      )),
      Rec(
        "z0",
        Str["id"],
        Str:Str["id":"z0"],
        Str:Str[:]
      ))

    verifyEq(
      Rec.fromDict(
        Etc.dict3(
          "id", ref("z0"),
          "a", "x",
          "b", Etc.dict2(
            "c", "y",
            "d", ref("z1"))
      )),
      Rec(
        "z0",
        Str["id", "a", "b", "b.c", "b.d"],
        Str:Str["id":"z0", "b.d":"z1"],
        Str:Str["a":"x", "b.c":"y"]
      ))
  }

  private static Ref ref(Str str) { Ref.fromStr(str) }

  private TestData testData := TestData()
}

