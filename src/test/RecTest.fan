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
        Str:Str[:],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:]
      ))

    verifyEq(
      Rec.fromDict(
        Etc.dict5(
          "id", ref("z0"),
          "a", "x",
          "b", Etc.dict3(
            "c", "y",
            "d", ref("z1"),
            "e", M
          ),
          "f", M,
          "g", Etc.dict1(
            "h", Etc.dict1(
              "i", Coord(37.0f, 77.0f)
            )
          )
        )
      ),
      Rec(
        "z0",
        Str["id", "a", "b", "b.c", "b.d", "b.e", "f", "g", "g.h", "g.h.i"],
        Str:Str["id":"z0", "b.d":"z1"],
        Str:Str["a":"x", "b.c":"y"],
        Str:Float[:],
        Str:Str[:],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
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
        Str:Str["a":"_", "b.c":"m", "b.d":"_", "b.e":"F"],
        Str:Bool[:],
        Str:Str[:],
        Str:Str[:],
        Str:Str[:]
      )
    )

    verifyEq(
      Rec.fromDict(
        Etc.dict5(
          "id", ref("z0"),
          "a", true,
          "b", `https://project-haystack.org/`,
          "c", Date.fromStr("2021-03-22"),
          "d", Time.fromStr("17:19:23")
        )
      ),
      Rec(
        "z0",
        Str["id", "a", "b", "c", "d"],
        Str:Str["id":"z0"],
        Str:Str[:],
        Str:Float[:],
        Str:Str[:],
        Str:Bool["a":true],
        Str:Str["b":"https://project-haystack.org/"],
        Str:Str["c":"2021-03-22"],
        Str:Str["d":"17:19:23"]
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

