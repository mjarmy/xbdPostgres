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
        Str:Str[:],
        Str:Float[:],
        Str:Str[:]
      ))

    verifyEq(
      Rec.fromDict(
        Etc.dict4(
          "id", ref("z0"),
          "a", "x",
          "b", Etc.dict3(
            "c", "y",
            "d", ref("z1"),
            "e", M
          ),
          "f", M
        )
      ),
      Rec(
        "z0",
        Str["id", "a", "b", "b.c", "b.d", "b.e", "f"],
        Str:Str["id":"z0", "b.d":"z1"],
        Str:Str["a":"x", "b.c":"y"],
        Str:Float[:],
        Str:Str[:]
      )
    )

    verifyEq(
      Rec.fromDict(
        Etc.dict3(
          "id", ref("z0"),
          "a", n(1.0f),
          "b", Etc.dict3(
            "c", n(2.0f, "m"),
            "d", n(3),
            "e", n(4, "F")
          )
        )
      ),
      Rec(
        "z0",
        Str["id", "a", "b", "b.c", "b.d", "b.e"],
        Str:Str["id":"z0"],
        Str:Str[:],
        Str:Float["a":1.0f, "b.c":2.0f, "b.d":3.0f, "b.e":4.0f],
        Str:Str["a":"_", "b.c":"m", "b.d":"_", "b.e":"F"]
      )
    )
  }

  private static Marker M() { Marker.val }
  private static Ref ref(Str str) { Ref.fromStr(str) }
  private static Number n(Num val, Str? unit := null)
  {
    Number.makeNum(val, unit == null ? null : Unit.fromStr(unit))
  }

  private TestData testData := TestData()
}

